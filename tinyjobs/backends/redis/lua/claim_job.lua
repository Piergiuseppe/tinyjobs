local worker, lease_expires_at = ARGV[2], lease_until(ARGV[3])
local job = load(ARGV[4])
if not job or job.status ~= 'queued' or tonumber(job.run_at) > tonumber(HORIZON) then
  return {}
end
take(job, worker, lease_expires_at, ARGV[5])
return { flatten(job) }
