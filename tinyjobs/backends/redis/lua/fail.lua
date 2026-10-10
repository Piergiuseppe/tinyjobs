local job = held_by(ARGV[2], ARGV[3])
if not job then
  return 0
end
local summary, execution_status, error_type, traceback, retry_at = ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8]
if retry_at == '' then
  transition(job, 'failed', { finished_at = NOW, last_error = summary }, { 'lease_expires_at' })
else
  transition(job, 'queued', { run_at = retry_at, last_error = summary }, { 'lease_expires_at', 'worker_id' })
end
record_execution(job.id, job.attempts, {
  status = execution_status, finished_at = NOW, error_type = error_type, traceback = traceback,
})
return 1
