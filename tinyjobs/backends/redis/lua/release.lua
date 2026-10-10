local job = held_by(ARGV[2], ARGV[3])
if not job then
  return 0
end
transition(job, 'queued', { run_at = ARGV[4] }, { 'lease_expires_at', 'worker_id' })
close_running_execution(job, 'lost')
return 1
