local name, expected, next_run_at = ARGV[2], ARGV[3], ARGV[4]
local schedule = key('schedule', name)
if redis.call('HGET', schedule, 'next_run_at') ~= expected then
  return false
end
redis.call('HSET', schedule, 'next_run_at', next_run_at)
local stored = {}
local position = 6
for _ = 1, tonumber(ARGV[5]) do
  local job
  job, position = read_job(ARGV, position)
  stored[#stored + 1] = insert(job)
end
if #stored > 0 then
  redis.call('HSET', schedule, 'last_run_at', NOW, 'last_job_id', stored[#stored])
end
return stored
