local status, cutoff, batch = ARGV[2], ARGV[3], tonumber(ARGV[4])
local doomed = redis.call('ZRANGEBYSCORE', key('finished', status), '-inf', '(' .. cutoff, 'LIMIT', 0, batch)
for _, id in ipairs(doomed) do
  local job = load(id)
  leave(job)
  redis.call('ZREM', key('index', 'all'), id)
  redis.call('ZREM', key('index', 'queue', job.queue), id)
  redis.call('DEL', job_key(id), key('executions', id))
end
return #doomed
