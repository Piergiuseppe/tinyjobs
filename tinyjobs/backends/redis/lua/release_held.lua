local batch, queue_count = tonumber(ARGV[2]), tonumber(ARGV[3])
local given = nil
if queue_count >= 0 then
  given = {}
  for index = 4, 3 + queue_count do
    given[#given + 1] = ARGV[index]
  end
end
local released, examined = 0, 0
for index = 4 + math.max(queue_count, 0), #ARGV do
  local task = ARGV[index]
  for _, queue in ipairs(given or redis.call('SMEMBERS', key('held_queues', task))) do
    if examined >= batch then
      break
    end
    for _, member in ipairs(redis.call('ZRANGE', held_line(task, queue), 0, batch - examined - 1)) do
      local job = held_entry(task, queue, member)
      if job then
        transition(job, 'queued', {})
        released = released + 1
      end
      examined = examined + 1
    end
  end
  if examined >= batch then
    break
  end
end
return { released, examined }
