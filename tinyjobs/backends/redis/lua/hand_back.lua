local job = held_by(ARGV[2], ARGV[3], ARGV[4])
if not job then
  return 0
end
redis.call('HDEL', key('executions', job.id), claim_of(job))
transition(job, 'queued', {
  run_at = ARGV[5],
  attempts = tostring(tonumber(job.attempts) - 1),
  hand_backs = tostring(tonumber(job.hand_backs or '0') + 1),
  claims = claim_of(job),
}, { 'lease_expires_at', 'worker_id' })
return 1
