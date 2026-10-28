local expires = lease_until(ARGV[2])
local renewed = {}
for index = 4, #ARGV do
  local job = load(ARGV[index])
  if job and job.status == 'running' and (ARGV[3] == '' or job.worker_id == ARGV[3]) then
    redis.call('HSET', job_key(job.id), 'lease_expires_at', expires)
    redis.call('ZADD', key('leases'), expires, job.id)
    renewed[#renewed + 1] = job.id
  end
end
return renewed
