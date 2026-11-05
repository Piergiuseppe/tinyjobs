local task, cutoff, batch, since = ARGV[2], no_later(ARGV[3]), tonumber(ARGV[4]), no_later(ARGV[5])
local results = key('results', task)
local expiring = redis.call('ZRANGEBYSCORE', results, since, '(' .. cutoff, 'LIMIT', 0, batch)
for _, id in ipairs(expiring) do
  redis.call('HDEL', job_key(id), 'result')
  redis.call('ZREM', results, id)
end
return #expiring
