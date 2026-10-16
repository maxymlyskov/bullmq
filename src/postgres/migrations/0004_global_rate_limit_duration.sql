-- BullMQ PostgreSQL backend — global rate limit duration.
--
-- Replaces `move_to_active` (created in 0002_functions.sql) so a global rate
-- limit (`Queue.setGlobalRateLimit`) is applied as a pair: when the queue meta
-- holds a `max`, its `duration` is used too. Before, the meta `max` was
-- counted over the worker's own `limiter.duration`, so a worker with a shorter
-- window exceeded the global rate. Mirrors moveToActive-11.lua and
-- fetchNextJob.lua.
--
-- Only the function body changes — no schema/DDL change — so the signature is
-- identical and older clients keep working against the updated schema.

CREATE OR REPLACE FUNCTION move_to_active(
  p_queue            text,
  p_token            text,
  p_lock_ms          bigint,
  p_now              bigint,
  p_name             text,
  p_limiter_max      integer,
  p_limiter_duration bigint
) RETURNS SETOF job
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
  v_id       text;
  v_job      job;
  v_meta_max integer;
  v_max      integer;
  v_duration bigint;
  v_concurrency integer;
BEGIN
  -- Promote due delayed jobs first, regardless of pause. Redis promotes delayed
  -- jobs via a delay timer that routes through getTargetQueueList, so while the
  -- queue is paused they land in the paused list and still surface (counted as
  -- paused) even though they are not processed. Running this before the pause
  -- early-return reproduces that: the jobs become 'waiting' (reported as paused)
  -- but the claim/activate below is skipped.
  FOR v_id IN
    WITH promoted AS (
      UPDATE job
         SET state = 'waiting', delay_ms = 0
       WHERE queue = p_queue
         AND state = 'delayed'
         AND process_at_ms <= p_now
      RETURNING id
    )
    SELECT id FROM promoted
  LOOP
    PERFORM publish_event(p_queue, 'waiting',
      jsonb_build_object('jobId', v_id, 'prev', 'delayed'));
  END LOOP;

  IF EXISTS (
    SELECT 1 FROM meta
     WHERE queue = p_queue AND field = 'paused' AND value = '1'
  ) THEN
    RETURN;
  END IF;

  -- Global concurrency: never run more than meta.concurrency jobs active at
  -- once (mirrors isQueueMaxed). Serialized per queue with a transaction-scoped
  -- advisory lock so concurrent workers cannot all pass the check and overshoot;
  -- it releases at commit, once this claim is reflected in the active count.
  SELECT value::integer INTO v_concurrency
    FROM meta WHERE queue = p_queue AND field = 'concurrency';
  IF v_concurrency IS NOT NULL THEN
    PERFORM pg_advisory_xact_lock(hashtext('bullmq:concurrency:' || p_queue));
    IF (
      SELECT count(*) FROM job
       WHERE queue = p_queue AND state = 'active'
    ) >= v_concurrency THEN
      RETURN;
    END IF;
  END IF;

  -- Rate limit: meta `max` wins, else the worker's `limiter.max`.
  SELECT value::integer INTO v_meta_max
    FROM meta WHERE queue = p_queue AND field = 'max';
  v_max := COALESCE(v_meta_max, p_limiter_max);

  IF v_max IS NOT NULL THEN
    -- Redis evaluates moveToActive atomically on a single-threaded server, so a
    -- worker's limiter check and its token consumption can never interleave with
    -- another worker's. Postgres runs each worker's call under its own snapshot,
    -- so without serialization several concurrent workers could all read
    -- counter < max, each claim a (distinct) job, and each consume a token —
    -- overshooting the limit. A transaction-scoped advisory lock serializes the
    -- check-and-consume per queue exactly like Redis; under READ COMMITTED the
    -- effective check below then runs on a fresh snapshot that sees the token a
    -- preceding worker just committed. The lock auto-releases at statement end
    -- (this function is always called standalone) and is only taken when a
    -- limiter actually applies, so unlimited queues pay nothing.
    PERFORM pg_advisory_xact_lock(hashtext('bullmq:limiter:' || p_queue));
    IF rate_limit_effective(p_queue, p_limiter_max, p_now) > 0 THEN
      RETURN;
    END IF;
  END IF;

  SELECT id INTO v_id
    FROM job
   WHERE queue = p_queue AND state = 'waiting'
   ORDER BY priority, seq
   FOR UPDATE SKIP LOCKED
   LIMIT 1;

  IF v_id IS NULL THEN
    RETURN;
  END IF;

  -- Consume a token now that a job is actually being claimed. Duration: meta
  -- `duration` when meta `max` is set (the global limit is one pair), else the
  -- worker's `limiter.duration`.
  IF v_max IS NOT NULL THEN
    SELECT value::bigint INTO v_duration
      FROM meta WHERE queue = p_queue AND field = 'duration';
    IF v_meta_max IS NOT NULL THEN
      v_duration := COALESCE(v_duration, p_limiter_duration);
    ELSE
      v_duration := COALESCE(p_limiter_duration, v_duration);
    END IF;
    IF v_duration IS NOT NULL THEN
      PERFORM rate_limit_consume(p_queue, v_duration, p_now);
    END IF;
  END IF;

  UPDATE job
     SET state = 'active',
         lock_token = p_token,
         locked_until_ms = p_now + p_lock_ms,
         processed_at_ms = p_now,
         processed_by = p_name,
         attempts_started = attempts_started + 1
   WHERE queue = p_queue AND id = v_id
  RETURNING * INTO v_job;

  PERFORM publish_event(p_queue, 'active',
    jsonb_build_object('jobId', v_id, 'prev', 'waiting'));

  RETURN NEXT v_job;
END;
$$;
