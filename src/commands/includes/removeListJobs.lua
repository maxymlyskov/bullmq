--[[
  Functions to remove jobs.
]]

-- Includes
--- @include "filterOutJobsToIgnore"
--- @include "removeJobs"

local function getListItems(keyName, max)
  return rcall('LRANGE', keyName, 0, max - 1)
end

local function removeListJobs(keyName, hard, baseKey, max, jobsToIgnore)
  local items = getListItems(keyName, max)
  local jobs = items

  if jobsToIgnore then
    jobs = filterOutJobsToIgnore(jobs, jobsToIgnore)
  end

  local count = removeJobs(jobs, hard, baseKey, max)
  rcall("LTRIM", keyName, #items, -1)

  -- Put the ignored jobs back at the head, keeping their order.
  if jobsToIgnore and #jobs < #items then
    for i = #items, 1, -1 do
      if jobsToIgnore[items[i]] then
        rcall("LPUSH", keyName, items[i])
      end
    end
  end
  return count
end
