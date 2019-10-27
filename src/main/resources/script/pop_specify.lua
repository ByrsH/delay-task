local sourceKey = KEYS[1]
local targetKey = KEYS[2]
local member = ARGV[1]
local now = ARGV[2]

local number = redis.call('ZREM', sourceKey, member)
if(number == 1) then
    return redis.call('ZADD', targetKey, now, member)
else
    return number
end