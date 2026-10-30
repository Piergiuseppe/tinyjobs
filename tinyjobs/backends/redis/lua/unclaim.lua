local held = key('claim', ARGV[2])
local returned = 0
for _, id in ipairs(redis.call('SMEMBERS', held)) do
  local job = load(id)
  if job and job.status == 'running' and job.claim_token == ARGV[2] then
    redis.call('HDEL', key('executions', job.id), claim_of(job))
    transition(job, 'queued', {
      attempts = tostring(tonumber(job.attempts) - 1),
      claims = claim_of(job),
    }, { 'lease_expires_at', 'worker_id', 'claim_token' })
    returned = returned + 1
  end
end
redis.call('DEL', held)
return returned
