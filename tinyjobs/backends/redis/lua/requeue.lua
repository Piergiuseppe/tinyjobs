local job = load(ARGV[2])
if not job or (job.status ~= 'failed' and job.status ~= 'cancelled') then
  return 0
end
transition(job, 'queued', { run_at = NOW, attempts = '0' },
  { 'finished_at', 'lease_expires_at', 'worker_id', 'last_error' })
hold_dedup(job)
return 1
