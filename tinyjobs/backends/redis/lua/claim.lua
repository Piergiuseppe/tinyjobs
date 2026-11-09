local worker, lease_expires_at, limit = ARGV[2], lease_until(ARGV[3]), tonumber(ARGV[4])
local token = ARGV[5]
local PROMOTE_BATCH = 100
local SLICES = 4
local queue_count = tonumber(ARGV[6])
local queues, wanted = {}, {}
for index = 7, 6 + queue_count do
  if not wanted[ARGV[index]] then
    queues[#queues + 1] = ARGV[index]
    wanted[ARGV[index]] = true
  end
end
local limits = {}
for index = 7 + queue_count, #ARGV, 3 do
  limits[ARGV[index]] = { concurrency = tonumber(ARGV[index + 1]), interval = tonumber(ARGV[index + 2]) }
end
local started = {}

local function running_of(task)
  return tonumber(redis.call('HGET', key('running_tasks'), task) or '0')
end

local function room_of(task)
  local rule = limits[task]
  if not rule then
    return limit
  end
  if rule.interval then
    local last_at = tonumber(redis.call('HGET', key('limit', task), 'last_at') or '0')
    if started[task] or tonumber(HORIZON) < last_at + rule.interval then
      return 0
    end
  end
  local room = limit
  if rule.concurrency then
    room = math.min(room, rule.concurrency - running_of(task))
  end
  if rule.interval then
    room = math.min(room, 1)
  end
  return math.max(room, 0)
end

local function ordered(left, right)
  if left.score ~= right.score then
    return left.score < right.score
  end
  return left.member < right.member
end


for _, queue in ipairs(queues) do
  local due = redis.call('ZRANGEBYSCORE', key('waiting', queue), '-inf', HORIZON, 'LIMIT', 0, PROMOTE_BATCH)
  for _, id in ipairs(due) do
    local job = load(id)
    redis.call('ZREM', key('waiting', queue), id)
    redis.call('ZADD', key('ready', queue), -tonumber(job.priority), ready_member(job))
  end
end

local claimed = {}
for _ = 1, SLICES do
  local room = limit - #claimed
  local read_rows, frontier = {}, nil
  for _, queue in ipairs(queues) do
    local rows = redis.call('ZRANGE', key('ready', queue), 0, room - 1, 'WITHSCORES')
    for position = 1, #rows, 2 do
      read_rows[#read_rows + 1] = { score = tonumber(rows[position + 1]), member = rows[position] }
    end
    if #rows == 2 * room then
      local last = { score = tonumber(rows[#rows]), member = rows[#rows - 1] }
      if not frontier or ordered(last, frontier) then
        frontier = last
      end
    end
  end
  for task in pairs(limits) do
    local task_room = math.min(room_of(task), room)
    if task_room > 0 then
      for _, queue in ipairs(queues) do
        for _, candidate in ipairs(held_jobs(task, queue, task_room)) do
          read_rows[#read_rows + 1] = candidate
        end
      end
    end
  end
  local candidates = {}
  for _, candidate in ipairs(read_rows) do
    if not frontier or not ordered(frontier, candidate) then
      candidates[#candidates + 1] = candidate
    end
  end
  table.sort(candidates, ordered)
  local holding = false
  for _, candidate in ipairs(candidates) do
    if #claimed >= limit then
      break
    end
    local job = candidate.job or load(member_id(candidate.member))
    if room_of(job.task) > 0 then
      take(job, worker, lease_expires_at, token)
      local rule = limits[job.task]
      if rule and rule.interval then
        started[job.task] = true
        redis.call('HSET', key('limit', job.task), 'last_at', HORIZON)
      end
      claimed[#claimed + 1] = packed(job, 'claim_token')
    elseif not candidate.job then
      transition(job, 'held', {})
      holding = true
    end
  end
  if not holding or not frontier or #claimed >= limit then
    break
  end
end
return claimed
