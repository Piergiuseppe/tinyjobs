local token = ARGV[2]
local held_from, held_to = '[' .. token .. '|', '[' .. token .. '|\255'
local returned = 0
for _, held in ipairs(redis.call('ZRANGEBYLEX', key('claim_tokens'), held_from, held_to)) do
  local job = load(string.sub(held, #token + 2))
  if job and job.status == 'running' and job.claim_token == token then
    redis.call('HDEL', key('executions', job.id), claim_of(job))
    transition(job, 'queued', {
      attempts = tostring(tonumber(job.attempts) - 1),
      claims = claim_of(job),
    }, { 'lease_expires_at', 'worker_id' })
    returned = returned + 1
  else
    redis.call('ZREM', key('claim_tokens'), held)
  end
end
return returned
