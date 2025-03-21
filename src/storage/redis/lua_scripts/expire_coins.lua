-- Copyright (c) Mysten Labs, Inc.
-- SPDX-License-Identifier: Apache-2.0

-- This script is used to expire gas coins that have been reserved but not used after the expiration time.
-- It takes out all gas coins from the expiration_queue that have expired and returns them to the caller.
-- The first argument is the sponsor's address.
-- The second argument is the current timestamp.

local sponsor_addresses = cjson.decode(ARGV[1])
local current_time = tonumber(ARGV[2])

local expired_reservations = {}

for _, sponsor_address in ipairs(sponsor_addresses) do
    local t_expiration_queue = sponsor_address .. ':expiration_queue'
    local elements = redis.call('ZRANGEBYSCORE', t_expiration_queue, 0, current_time)

    if #elements > 0 then
        for _, reservation_id in ipairs(elements) do
            local key = sponsor_address .. ':' .. reservation_id
            local object_ids = redis.call('GET', key)
            if object_ids then
                redis.call('DEL', key)
                table.insert(expired_reservations, object_ids)
            end
        end
        redis.call('ZREMRANGEBYSCORE', t_expiration_queue, 0, current_time)
    end
end

return expired_reservations
