for index = 2, #ARGV do
  local job = load(ARGV[index])
  if job then
    claim_of(job)
  end
end
return 0
