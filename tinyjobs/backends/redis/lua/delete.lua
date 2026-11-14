local job = load(ARGV[2])
if not job or not ACTIVE[job.status] or job.status == 'running' then
  return 0
end
if redis.call('SCARD', key('children', job.id)) > 0 then
  return 0
end
forget(job)
return 1
