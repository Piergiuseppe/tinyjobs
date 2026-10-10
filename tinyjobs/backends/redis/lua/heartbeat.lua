for index = 3, #ARGV do
  local job = load(ARGV[index])
  if job and job.status == 'running' then
    redis.call('HSET', job_key(job.id), 'lease_expires_at', ARGV[2])
    redis.call('ZADD', key('leases'), ARGV[2], job.id)
  end
end
return 0
