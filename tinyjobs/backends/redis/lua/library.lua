local PREFIX = KEYS[1]
local NOW = ARGV[1]
local ACTIVE = { queued = true, running = true }
local TERMINAL = { success = true, failed = true, cancelled = true }

local function key(...)
  return PREFIX .. ':' .. table.concat({ ... }, ':')
end

local function job_key(id)
  return key('job', id)
end

local function load(id)
  local flat = redis.call('HGETALL', job_key(id))
  if #flat == 0 then
    return nil
  end
  local job = {}
  for index = 1, #flat, 2 do
    job[flat[index]] = flat[index + 1]
  end
  return job
end

local function flatten(job)
  local flat = {}
  for field, value in pairs(job) do
    flat[#flat + 1] = field
    flat[#flat + 1] = value
  end
  return flat
end

local function read_job(arguments, start)
  local size = tonumber(arguments[start])
  local job = {}
  for index = start + 1, start + size, 2 do
    job[arguments[index]] = arguments[index + 1]
  end
  return job, start + size + 1
end

local function ready_member(job)
  return string.format('%020.6f', tonumber(job.run_at)) .. '|' .. job.id
end

local function adjust(hash, field, delta)
  if redis.call('HINCRBY', hash, field, delta) <= 0 then
    redis.call('HDEL', hash, field)
  end
end

local function release_dedup(job)
  if job.dedup_key and redis.call('GET', key('dedup', job.dedup_key)) == job.id then
    redis.call('DEL', key('dedup', job.dedup_key))
  end
end

local function hold_dedup(job)
  if job.dedup_key then
    redis.call('SET', key('dedup', job.dedup_key), job.id, 'NX')
  end
end

local function enter(job)
  local status = job.status
  adjust(key('counts'), job.queue .. '\t' .. status, 1)
  redis.call('ZADD', key('index', 'status', status), 0, job.id)
  if status == 'queued' then
    adjust(key('queued_tasks'), job.task, 1)
    redis.call('ZADD', key('queued', job.queue), job.run_at, job.id)
    if tonumber(job.run_at) <= tonumber(NOW) then
      redis.call('ZADD', key('ready', job.queue), -tonumber(job.priority), ready_member(job))
    else
      redis.call('ZADD', key('waiting', job.queue), job.run_at, job.id)
    end
  elseif status == 'running' then
    redis.call('ZADD', key('leases'), job.lease_expires_at, job.id)
  elseif TERMINAL[status] then
    redis.call('ZADD', key('finished', status), job.finished_at, job.id)
    release_dedup(job)
  end
end

local function leave(job)
  local status = job.status
  adjust(key('counts'), job.queue .. '\t' .. status, -1)
  redis.call('ZREM', key('index', 'status', status), job.id)
  if status == 'queued' then
    adjust(key('queued_tasks'), job.task, -1)
    redis.call('ZREM', key('queued', job.queue), job.id)
    redis.call('ZREM', key('waiting', job.queue), job.id)
    redis.call('ZREM', key('ready', job.queue), ready_member(job))
  elseif status == 'running' then
    redis.call('ZREM', key('leases'), job.id)
  elseif TERMINAL[status] then
    redis.call('ZREM', key('finished', status), job.id)
  end
end

local function transition(job, status, changes, removals)
  leave(job)
  changes.status = status
  for field, value in pairs(changes) do
    job[field] = value
  end
  redis.call('HSET', job_key(job.id), unpack(flatten(changes)))
  if removals and #removals > 0 then
    for _, field in ipairs(removals) do
      job[field] = nil
    end
    redis.call('HDEL', job_key(job.id), unpack(removals))
  end
  enter(job)
end

local function insert(job)
  if job.dedup_key then
    local holder = redis.call('GET', key('dedup', job.dedup_key))
    if holder then
      local existing = load(holder)
      if existing and ACTIVE[existing.status] then
        return holder
      end
    end
  end
  redis.call('HSET', job_key(job.id), unpack(flatten(job)))
  redis.call('ZADD', key('index', 'all'), 0, job.id)
  redis.call('ZADD', key('index', 'queue', job.queue), 0, job.id)
  if ACTIVE[job.status] and job.dedup_key then
    redis.call('SET', key('dedup', job.dedup_key), job.id)
  end
  enter(job)
  return job.id
end

local function held_by(id, worker)
  local job = load(id)
  if job and job.status == 'running' and job.worker_id == worker then
    return job
  end
  return nil
end

local function record_execution(id, attempt, fields)
  local stored = redis.call('HGET', key('executions', id), attempt)
  local execution = stored and cjson.decode(stored) or {}
  for field, value in pairs(fields) do
    execution[field] = value
  end
  redis.call('HSET', key('executions', id), attempt, cjson.encode(execution))
end

local function start_execution(job)
  redis.call('HSET', key('executions', job.id), job.attempts, cjson.encode({
    status = 'running', worker_id = job.worker_id, started_at = NOW,
  }))
end

local function close_running_execution(job, status)
  local stored = redis.call('HGET', key('executions', job.id), job.attempts)
  if stored and cjson.decode(stored).status == 'running' then
    record_execution(job.id, job.attempts, { status = status, finished_at = NOW })
  end
end
