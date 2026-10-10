local last = tonumber(redis.call('GET', key('last_purge_at')) or '0')
if last ~= 0 and last > tonumber(ARGV[2]) then
  return 0
end
redis.call('SET', key('last_purge_at'), NOW)
return 1
