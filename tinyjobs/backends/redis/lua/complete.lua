local job = held_by(ARGV[2], ARGV[3])
if not job then
  return 0
end
local changes = { finished_at = NOW }
local removals = { 'lease_expires_at', 'last_error' }
if ARGV[4] == '1' then
  changes.result = ARGV[5]
end
transition(job, 'success', changes, removals)
record_execution(job.id, job.attempts, { status = 'success', finished_at = NOW })
return 1
