local released = 0
for index = 2, #ARGV, 4 do
  local job = held_by(ARGV[index], ARGV[index + 1], ARGV[index + 2])
  if job then
    transition(job, 'queued', { run_at = ARGV[index + 3] }, { 'lease_expires_at', 'worker_id', 'hand_backs' })
    close_running_execution(job, 'lost')
    released = released + 1
  end
end
return released
