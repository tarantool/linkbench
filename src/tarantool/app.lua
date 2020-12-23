#!/usr/bin/env tarantool

local function get_env(key, t, default)
    assert(key, "key required")
    assert(t,   "type required")
    if default ~= nil and type(default) ~= t then
        error(("default is not of corresponding type for key='%s'"):format(
            key
        ))
    end

    local value = os.getenv(key)
    if value == nil then
        if default == nil then
            error(("env key='%s' was not specified"):format(key))
        end

        return default
    end

    if t == 'number' then
        return tonumber(value)
    elseif t == 'boolean' then
        return tonumber(value:upper()) == 'TRUE'
    else
        return value
    end
end

local path = require('fio').dirname(arg[0])
package.path = path.."/?.lua;"..package.path
package.cpath = path.."/?.so;"..package.cpath

require('console').listen('unix/:./tarantool.sock')
require('gperftools').cpu.start('tarantool.prof')

box.cfg{
    listen       = get_env('TT_LISTEN',       'number', 3301),
    vinyl_memory = get_env('TT_VINYL_MEMORY', 'number', 512 * 1024^2),
    vinyl_cache  = get_env('TT_VINYL_CACHE',  'number', 2 * 1024^3),
}

require('app_internal')

-- vim:ts=4:sw=4:expandtab
