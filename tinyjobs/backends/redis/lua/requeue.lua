local job = load(ARGV[2])
if not job or (job.status ~= 'failed' and job.status ~= 'cancelled') then
  return 0
end
if dedup_holder(job) then
  return 0
end
local status = 'queued'
if job.after then
  status = placed_after(parent_status(job) or 'success')
  if status == 'cancelled' then
    return 0
  end
end
local claims = claim_of(job)
transition(job, status, {
  run_at = tonumber(claims) == 0 and job.created_at or NOW, attempts = '0', claims = claims,
},
  { 'finished_at', 'lease_expires_at', 'worker_id', 'last_error' })
hold_dedup(job)
return 1
