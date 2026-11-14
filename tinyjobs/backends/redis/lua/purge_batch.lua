local status, cutoff, batch = ARGV[2], no_later(ARGV[3]), tonumber(ARGV[4])
local doomed = redis.call('ZRANGEBYSCORE', key('finished', status), '-inf', '(' .. cutoff, 'LIMIT', 0, batch)
for _, id in ipairs(doomed) do
  forget(load(id))
end
return #doomed
