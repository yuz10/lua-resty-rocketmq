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
    local running = true
    if self.processor.processHangingRequest then
        ngx.thread.spawn(function()
            while running do
                self.processor:processHangingRequest(sock)
                ngx.sleep(0.1)
            end
        end)
    end

    while true do
        local recv, err = sock:receive(4)
        if not recv then
            running = false
            return nil, err
        end
        local length = core.getInt(recv, 1)
        recv, err = sock:receive(length)
        if not recv then
            running = false
            return nil, err
        end

        local header, header_length = core.decodeHeader(recv)
        local body = string.sub(recv, header_length + 5)
        local addr = var.remote_addr
        self.processor:processRequest(sock, addr, header, body)
    end
end

return _M
