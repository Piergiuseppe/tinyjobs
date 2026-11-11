local jobs = {}
local position = 3
for index = 1, tonumber(ARGV[2]) do
  jobs[index], position = read_job(ARGV, position)
end
local missing = orphan(jobs)
if missing then
  return { false, missing }
end
local stored, placed = {}, {}
for index, job in ipairs(jobs) do
  stored[index] = insert(job)
  placed[index] = job.status
end
return { stored, placed }
