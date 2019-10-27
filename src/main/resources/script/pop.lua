local sourceKey = KEYS[1]
local targetKey = KEYS[2]
local number = ARGV[1]
local now = ARGV[2]

local payload = nil

if(number > '0') then
        payload = redis.call('ZRANGEBYSCORE', sourceKey, '-inf', now, 'LIMIT', '0', number)
        local size = table.getn(payload)
        if(size > 0) then
            for i,v in ipairs(payload) do
                redis.call('ZADD', targetKey, now, v)
            end
            redis.call('ZREMRANGEBYRANK', sourceKey, '0', size)
        end
        return payload
else
        return nil
end