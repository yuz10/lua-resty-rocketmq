local resty_hmac = require "resty.hmac"
local encode_base64 = ngx.encode_base64

local empty = function()
end

local function hmca_sha1_base64(key, content)
    local m = resty_hmac:new(key, resty_hmac.ALGOS.SHA1)
    m:update(content)
    return encode_base64(m:final())
end
local function new(accessKey, secretKey, securityToken)
    return {
        doBeforeRequest = function(addr, header, body)
            header.AccessKey = accessKey
            header.SecurityToken = securityToken
            local keys = {}
            for k, v in pairs(header) do
                if k ~= 'Signature' then
                    table.insert(keys, k)
                end
            end
            table.sort(keys)
            local content = ''
            for _, k in ipairs(keys) do
                content = content .. tostring(header[k])
            end
            if body then
                content = content .. body
            end
            header.Signature = hmca_sha1_base64(secretKey, content)
        end,
        doAfterResponse = empty,
    }
end

return {
    new = new
}
