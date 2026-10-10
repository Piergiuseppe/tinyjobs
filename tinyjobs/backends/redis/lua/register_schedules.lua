for index = 2, #ARGV, 4 do
  local name, cron, zone, next_run_at = ARGV[index], ARGV[index + 1], ARGV[index + 2], ARGV[index + 3]
  local schedule = key('schedule', name)
  local stored = redis.call('HMGET', schedule, 'cron', 'timezone')
  if stored[1] ~= cron or stored[2] ~= zone then
    redis.call('HSET', schedule, 'name', name, 'cron', cron, 'timezone', zone, 'next_run_at', next_run_at)
  end
  redis.call('SADD', key('schedules'), name)
end
return 0
