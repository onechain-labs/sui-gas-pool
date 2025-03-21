-- Copyright (c) Mysten Labs, Inc.
-- SPDX-License-Identifier: Apache-2.0

-- Release the lock for initializing a sponsor's account.
-- This is done by setting the lock expiration time to 0.
-- The first argument is the sponsor's address.

local sponsor_addresses = cjson.decode(ARGV[1])

for _, sponsor_address in ipairs(sponsor_addresses) do
    local t_init_lock = sponsor_address .. ':init_lock'
    redis.call('SET', t_init_lock, 0)
end

