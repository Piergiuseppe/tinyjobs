local worker, lease_expires_at, limit = ARGV[2], lease_until(ARGV[3]), tonumber(ARGV[4])
local PROMOTE_BATCH = 100
local SCAN_BATCH = 100
local named = tonumber(ARGV[5])
local tasks = nil
if named >= 0 then
  tasks = {}
  for index = 6, 5 + named do
    tasks[ARGV[index]] = true
  end
end
local candidates = {}
local function consider(ready, position)
  candidates[#candidates + 1] = { score = tonumber(ready[position + 1]), member = ready[position] }
end
for index = 6 + math.max(named, 0), #ARGV do
  local queue = ARGV[index]
  local due = redis.call('ZRANGEBYSCORE', key('waiting', queue), '-inf', HORIZON, 'LIMIT', 0, PROMOTE_BATCH)
  for _, id in ipairs(due) do
    local job = load(id)
    redis.call('ZREM', key('waiting', queue), id)
    redis.call('ZADD', key('ready', queue), -tonumber(job.priority), ready_member(job))
  end
  if tasks == nil then
    local ready = redis.call('ZRANGE', key('ready', queue), 0, limit - 1, 'WITHSCORES')
    for position = 1, #ready, 2 do
      consider(ready, position)
    end
  elseif named > 0 then
    local found, start = 0, 0
    while found < limit do
      local last = start + SCAN_BATCH - 1
      local ready = redis.call('ZRANGE', key('ready', queue), start, last, 'WITHSCORES')
      if #ready == 0 then
        break
      end
      for position = 1, #ready, 2 do
        local id = string.match(ready[position], '|(.+)$')
        if found < limit and tasks[redis.call('HGET', job_key(id), 'task')] then
          consider(ready, position)
          found = found + 1
        end
      end
      start = start + SCAN_BATCH
    end
  end
end
table.sort(candidates, function(left, right)
  if left.score ~= right.score then
    return left.score < right.score
  end
  return left.member < right.member
end)
local claimed = {}
for index = 1, math.min(limit, #candidates) do
  local id = string.match(candidates[index].member, '|(.+)$')
  local job = load(id)
  transition(job, 'running', {
    worker_id = worker,
    attempts = tostring(tonumber(job.attempts) + 1),
    claims = tostring(tonumber(claim_of(job)) + 1),
    lease_expires_at = lease_expires_at,
  })
  start_execution(job)
  claimed[#claimed + 1] = flatten(job)
end
return claimed
