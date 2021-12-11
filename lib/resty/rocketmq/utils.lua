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
local floor = math.floor

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

local mon_lengths = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
-- Number of days in year until start of month; not corrected for leap years
local months_to_days_cumulative = { 0 }
for i = 2, 12 do
    months_to_days_cumulative[i] = months_to_days_cumulative[i - 1] + mon_lengths[i - 1]
end

local function is_leap(y)
    if (y % 4) ~= 0 then
        return false
    elseif (y % 100) ~= 0 then
        return true
    else
        return (y % 400) == 0
    end
end

local function leap_years_since(year)
    return floor(year / 4) - floor(year / 100) + floor(year / 400)
end

local function day_of_year(day, month, year)
    local yday = months_to_days_cumulative[month]
    if month > 2 and is_leap(year) then
        yday = yday + 1
    end
    return yday + day
end

local leap_years_since_1970 = leap_years_since(1970)
local function timestamp(year, month, day, hour, min, sec)
    local days_since_epoch = day_of_year(day, month, year)
            + 365 * (year - 1970)
            -- Each leap year adds one day
            + (leap_years_since(year - 1) - leap_years_since_1970) - 1

    return days_since_epoch * (60 * 60 * 24)
            + hour * (60 * 60)
            + min * 60
            + sec
end
_M.timestamp = timestamp

do
    local ip = char(127) .. char(0) .. char(0) .. char(1)
    local pid = ngx.worker.pid()
    local pidBin = char(band(rshift(pid, 8), 0xff)) .. char(band(pid, 0xff))
    local clientIdHash = char(random(0, 255)) .. char(random(0, 255)) .. char(random(0, 255)) .. char(random(0, 255))
    local counter = 0
    local timeDiffEightHours =  8 * 60 * 60
    local thisMonth = 0
    local nextMonth = 0

    _M.genUniqId = function()
        local time = ngx.now()
        if time >= nextMonth then
            local today = split(ngx.today(), "-")
            local y, m = tonumber(today[1]), tonumber(today[2])
            local y2, m2 = y, m + 1
            if m2 > 12 then
                m2 = 1
                y2 = y2 + 1
            end
            thisMonth = timestamp(y, m, 1, 0, 0, 0) - timeDiffEightHours
            nextMonth = timestamp(y2, m2, 1, 0, 0, 0) - timeDiffEightHours
        end
        time = (time - thisMonth) * 1000
        local timeBin = char(band(rshift(time, 24), 0xff)) ..
                char(band(rshift(time, 16), 0xff)) ..
                char(band(rshift(time, 8), 0xff)) ..
                char(band(time, 0xff))
        counter = counter + 1
        local counterBin = char(band(rshift(counter, 8), 0xff)) .. char(band(counter, 0xff))
        return bytes2string(ip .. pidBin .. clientIdHash .. timeBin .. counterBin)
    end
end

return _M
