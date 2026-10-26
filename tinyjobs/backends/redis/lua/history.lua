local job = load(ARGV[2])
if job then
  claim_of(job)
end
return redis.call('HGETALL', key('executions', ARGV[2]))
