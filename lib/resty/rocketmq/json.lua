local cjson_safe = require("cjson.safe")
local byte = string.byte
local sub = string.sub
local find = string.find

local function decode_string(s, i)
    local x = i + 1
    while x < #s do
        if byte(s, x) == byte('\\') then
            x = x + 2
        elseif byte(s, x) == byte('"') then
            break
        else
            x = x + 1
        end
    end
    return cjson_safe.decode(sub(s, i, x)), x + 1
end

local function is_number(s, i)
    return byte(s, i) >= byte('0') and byte(s, i) <= byte('9')
            or byte(s, i) == byte('+') or byte(s, i) == byte('-')
            or byte(s, i) == byte('.') or byte(s, i) == byte('e') or byte(s, i) == byte('E')
end

local function decode_number(s, i)
    local x = i + 1
    while x <= #s do
        if is_number(s, x) then
            x = x + 1
        else
            break
        end
    end
    return cjson_safe.decode(sub(s, i, x - 1)), x
end

local function decode_const(s, i)
    for _, v in ipairs { 'true', 'false', 'null' } do
        if find(s, v, i, true) == i then
            return cjson_safe.decode(v), i + #v
        end
    end
    return nil, ('invalid token %s in %d'):format(s:sub(i, i), i)
end

local decode_array, decode_object

local function decode_any(s, i)
    if is_number(s, i) then
        return decode_number(s, i)
    elseif byte(s, i) == byte('"') then
        return decode_string(s, i)
    elseif byte(s, i) == byte('[') then
        return decode_array(s, i)
    elseif byte(s, i) == byte('{') then
        return decode_object(s, i)
    elseif byte(s, i) == byte('t') or byte(s, i) == byte('f') or byte(s, i) == byte('n') then
        return decode_const(s, i)
    end
    return nil, ('invalid token %s in %d'):format(s:sub(i, i), i)
end

decode_array = function(s, i)
    local x = i + 1
    local res = {}
    while x <= #s and byte(s, x) ~= byte(']') do
        local o
        o, x = decode_any(s, x)
        if o == nil then
            return nil, x
        end
        table.insert(res, o)
        if byte(s, x) == byte(',') then
            x = x + 1
        elseif byte(s, x) ~= byte(']') then
            return nil, ('invalid token %s in %d, , or ] expected'):format(s:sub(x, x), x)
        end
    end
    return res, x + 1
end

decode_object = function(s, i)
    local x = i + 1
    local res = {}
    while x <= #s and byte(s, x) ~= byte('}') do
        local k, v
        k, x = decode_any(s, x)
        if k == nil then
            return nil, x
        end
        if byte(s, x) ~= byte(':') then
            return nil, ('invalid token %s in %d, : expected'):format(s:sub(x, x), x)
        end
        x = x + 1
        v, x = decode_any(s, x)
        if v == nil then
            return nil, x
        end
        res[k] = v
        if byte(s, x) == byte(',') then
            x = x + 1
        elseif byte(s, x) ~= byte('}') then
            return nil, ('invalid token %s in %d, , or } expected'):format(s:sub(x, x), x)
        end
    end
    return res, x + 1
end

local function decode(s)
    local res, x = decode_any(s, 1)
    if res == nil then
        return nil, x
    end
    if x <= #s then
        return nil, ('invalid token %s in %d'):format(s:sub(x, x), x)
    end
    return res
end

return {
    decode = decode
}
