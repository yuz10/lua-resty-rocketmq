local resty_hmac = require "resty.hmac"
local encode_base64 = ngx.encode_base64

local empty = function()
end

local function hmac_sha1_base64(key, content)
    local m = resty_hmac:new(key, resty_hmac.ALGOS.SHA1)
    m:update(content)
    return encode_base64(m:final())
end

local _M = {}
local mt = { __index = _M }
function _M.doBeforeRequest(self, addr, header, body)
    if not header then
        return
    end
    header.AccessKey = self.accessKey
    header.SecurityToken = self.securityToken
    local keys = {}
    for k, v in pairs(header) do
        if k ~= 'Signature' and k ~= '_UNIQUE_KEY_QUERY' then
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
    header.Signature = hmac_sha1_base64(self.secretKey, content)
end
_M.doAfterResponse = empty

function _M.new(accessKey, secretKey, securityToken)
    return setmetatable({
        accessKey = accessKey,
        secretKey = secretKey,
        securityToken = securityToken
    }, mt)
end

return _M
