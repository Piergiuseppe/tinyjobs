local job = load(ARGV[2])
if not job or not ACTIVE[job.status] then
  return 0
end
transition(job, 'cancelled', { finished_at = NOW }, { 'lease_expires_at', 'hand_backs' })
close_running_execution(job, 'cancelled')
return 1
