local worker, lease_expires_at, limit = ARGV[2], lease_until(ARGV[3]), tonumber(ARGV[4])
local PROMOTE_BATCH = 100
local candidates = {}
for index = 5, #ARGV do
  local queue = ARGV[index]
  local due = redis.call('ZRANGEBYSCORE', key('waiting', queue), '-inf', HORIZON, 'LIMIT', 0, PROMOTE_BATCH)
  for _, id in ipairs(due) do
    local job = load(id)
    redis.call('ZREM', key('waiting', queue), id)
    redis.call('ZADD', key('ready', queue), -tonumber(job.priority), ready_member(job))
  end
  local ready = redis.call('ZRANGE', key('ready', queue), 0, limit - 1, 'WITHSCORES')
  for position = 1, #ready, 2 do
    candidates[#candidates + 1] = { score = tonumber(ready[position + 1]), member = ready[position] }
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
