local stored = tonumber(redis.call('GET', key('schema_version')) or '0')
local wanted = tonumber(ARGV[2])
if stored < wanted then
  if stored < 3 then
    recount_running_tasks()
  end
  redis.call('SET', key('schema_version'), ARGV[2])
  redis.call('SET', key('compatible_since'), ARGV[3])
  stored = wanted
end
return { stored, tonumber(redis.call('GET', key('compatible_since')) or stored) }
