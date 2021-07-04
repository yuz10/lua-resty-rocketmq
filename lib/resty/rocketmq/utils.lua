local bit = require("bit")
local tohex = bit.tohex
local band = bit.band
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift
local fmt = string.format
local random = math.random
local find, sub, append = string.find, string.sub, table.insert
local byte = string.byte
local char = string.char

local _M = {}
math.randomseed(ngx.now())

-- https://github.com/thibaultcha/lua-resty-jit-uuid/blob/master/lib/resty/jit-uuid.lua
function _M.uuid()
    return (fmt('%s%s%s%s-%s%s-%s%s-%s%s-%s%s%s%s%s%s',
            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2),

            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2),

            tohex(bor(band(random(0, 255), 0x0F), 0x40), 2),
            tohex(random(0, 255), 2),

            tohex(bor(band(random(0, 255), 0x3F), 0x80), 2),
            tohex(random(0, 255), 2),

            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2),
            tohex(random(0, 255), 2)))
end

-- https://github.com/lunarmodules/Penlight/blob/master/lua/pl/utils.lua
local function split(s, sep, n)
    local i1, ls = 1, {}
    if sep == '' then
        return { s }
    end
    while true do
        local i2, i3 = find(s, sep, i1, true)
        if not i2 then
            local last = sub(s, i1)
            if last ~= '' then
                append(ls, last)
            end
            if #ls == 1 and ls[1] == '' then
                return {}
            else
                return ls
            end
        end
        append(ls, sub(s, i1, i2 - 1))
        if n and #ls == n then
            ls[#ls] = sub(s, i1)
            return ls
        end
        i1 = i3 + 1
    end
end

_M.split = function(s, sep, n)
    if s == nil or s == '' then
        return {}
    end
    local ls = split(s, sep, n)
    if sep and sep ~= '' and find(s, sep, -#sep, true) then
        append(ls, "")
    end
    return ls
end

local h2b = {}
do
    local s = '0123456789'
    for i = 1, #s do
        h2b[byte(s, i)] = i - 1
    end

    s = 'ABCDEF'
    local s2 = s:lower()
    for i = 1, #s do
        h2b[byte(s, i)] = i + 10 - 1
        h2b[byte(s2, i)] = i + 10 - 1
    end
end

local function string2bytes(s)
    local res = {}
    for i = 1, #s / 2 do
        table.insert(res, char(h2b[byte(s, 2 * i - 1)] * 16 + h2b[byte(s, 2 * i)]))
    end
    return table.concat(res)
end

local b2h = '0123456789ABCDEF'
local function bytes2string(s)
    local res = {}
    for i = 1, #s do
        local b = byte(s, i)
        table.insert(res, char(byte(b2h, rshift(b, 4) + 1)))
        table.insert(res, char(byte(b2h, band(b, 0x0f) + 1)))
    end
    return table.concat(res)
end

local function toNumber(a)
    local res = 0
    for i = 1, #a do
        res = lshift(res, 8) + byte(a, i)
    end
    return res
end

local function toIp(a)
    if #a == 4 then
        --ipv4
        local res = {}
        for i = 1, 4 do
            table.insert(res, byte(a, i))
        end
        return table.concat(res, '.')
    end
    -- ipv6
    local res = {}
    for i = 1, #a / 2 do
        table.insert(res, tohex(a:sub(i * 2 - 1, i * 2)))
    end
    return '[' .. table.concat(res, ':') .. ']'

end
_M.toIp = toIp

function _M.createMessageId(ip, port, offset)
    local portInt = {}
    for i = 1, 4 do
        portInt[5 - i] = char(band(port, 0xff))
        port = rshift(port, 8)
    end

    local offsetLong = {}
    for i = 1, 8 do
        offsetLong[9 - i] = char(band(offset, 0xff))
        offset = rshift(offset, 8)
    end

    return bytes2string(ip .. table.concat(portInt) .. table.concat(offsetLong))
end

function _M.decodeMessageId(msgId)
    local ipLength = #msgId == 32 and 4 * 2 or 16 * 2
    local ip = string2bytes(msgId:sub(1, ipLength))
    local port = string2bytes(msgId:sub(ipLength + 1, ipLength + 8))
    local offset = string2bytes(msgId:sub(ipLength + 8 + 1, ipLength + 8 + 16))
    return toIp(ip) .. ':' .. toNumber(port), toNumber(offset)
end

function _M.string2messageProperties(str)
    local map = {}
    if not str or #str == 0 then
        return map
    end
    local array = split(str, string.char(2))
    for _, v in ipairs(array) do
        local spl = split(v, string.char(1), 2)
        map[spl[1]] = spl[2]
    end
    return map
end

return _M
