for index = 4, #ARGV do
  local job = load(ARGV[index])
  if job and job.status == 'running' and (ARGV[3] == '' or job.worker_id == ARGV[3]) then
    redis.call('HSET', job_key(job.id), 'lease_expires_at', ARGV[2])
    redis.call('ZADD', key('leases'), ARGV[2], job.id)
  end
end
return 0
