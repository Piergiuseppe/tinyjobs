local message = ARGV[2]
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
return #expired
