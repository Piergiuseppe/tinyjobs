local stored = tonumber(redis.call('GET', key('schema_version')) or '0')
local wanted = tonumber(ARGV[2])
if stored < wanted then
  redis.call('SET', key('schema_version'), ARGV[2])
  return wanted
end
return stored
