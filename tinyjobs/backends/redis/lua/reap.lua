local message = ARGV[2]
local RECOUNT_SECONDS = 60
local expired = redis.call('ZRANGEBYSCORE', key('leases'), '-inf', '(' .. HORIZON)
for _, id in ipairs(expired) do
  local job = load(id)
  if tonumber(job.attempts) < tonumber(job.max_attempts) then
    transition(job, 'queued', { run_at = HORIZON, last_error = message }, { 'lease_expires_at', 'worker_id', 'hand_backs' })
  else
    transition(job, 'failed', { finished_at = HORIZON, last_error = message }, { 'lease_expires_at', 'worker_id', 'hand_backs' })
  end
  close_running_execution(job, 'lost')
end
local recounted = tonumber(redis.call('GET', key('running_recounted_at')) or '0')
if math.abs(tonumber(HORIZON) - recounted) >= RECOUNT_SECONDS then
  recount_running_tasks()
  redis.call('SET', key('running_recounted_at'), HORIZON)
end
return #expired
