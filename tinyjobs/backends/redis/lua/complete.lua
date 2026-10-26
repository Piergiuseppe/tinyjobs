local job = held_by(ARGV[2], ARGV[3], ARGV[4])
if not job then
  return 0
end
local changes = { finished_at = NOW }
local removals = { 'lease_expires_at', 'last_error', 'hand_backs' }
if ARGV[5] == '1' then
  changes.result = ARGV[6]
end
transition(job, 'success', changes, removals)
record_execution(job.id, claim_of(job), { status = 'success', finished_at = NOW })
return 1
