local cjson_safe = require("cjson.safe")
local core = require("resty.rocketmq.core")
local ngx = ngx
local var = ngx.var

---@class remoting
local _M = {}
_M.__index = _M
function _M.new(processor)
    ---@type remoting
    local remoting = setmetatable({
        processor = processor
    }, _M)
    return remoting
end

function _M:process()
    local sock = ngx.req.socket(true)
    while true do
        local recv, err = sock:receive(4)
        if not recv then
            return nil, err
        end
        local length = core.getInt(recv, 1)
        recv, err = sock:receive(length)
        if not recv then
            return nil, err
        end

        local header, header_length = core.decodeHeader(recv)
        local body = string.sub(recv, header_length + 5)
        local addr = var.remote_addr
        local resp = self.processor:processRequest(addr, header, body)
        local send = core.encode(resp.code, resp.header, resp.body, false, header.opaque)

        local ok, err = sock:send(send)
        if not ok then
            return nil, err
        end
    end

end

return _M
