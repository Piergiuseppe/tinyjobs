local stored = {}
local position = 3
for _ = 1, tonumber(ARGV[2]) do
  local job
  job, position = read_job(ARGV, position)
  stored[#stored + 1] = insert(job)
end
return stored
