local core = require("resty.rocketmq.core")
local remoting = require("resty.rocketmq.remoting")
local ngx = ngx

local RESPONSE_CODE = core.RESPONSE_CODE
local DLEDGER_REQUEST_CODE = {
    METADATA = 50000,
    APPEND = 50001,
    GET = 50002,
    VOTE = 51001,
    HEART_BEAT = 51002,
    PULL = 51003,
    PUSH = 51004,
    LEADERSHIP_TRANSFER = 51005,
}

local _M = {}
_M.__index = _M

function _M.new(master)
    local nameserver = setmetatable({
        master = master,
        queue = {},
        pending = {},
    }, _M)
    return remoting.new(nameserver)
end

local processors = {}
processors[DLEDGER_REQUEST_CODE.PUSH] = function(self, sock, addr, h, body)
    local header = h.extFields
    if self.master then
        local s = core.encode(RESPONSE_CODE.SYSTEM_ERROR, { remark = "cant push to master" }, nil, false, h.opaque)
        sock:send(s)
        return
    end
    self.queue[tonumber(header.index)] = body
    local s = core.encode(RESPONSE_CODE.SUCCESS, {}, nil, false, h.opaque)
    sock:send(s)
end

processors[DLEDGER_REQUEST_CODE.APPEND] = function(self, sock, addr, h, body)
    if not self.master then
        local s = core.encode(RESPONSE_CODE.SYSTEM_ERROR, { remark = "cant append to slave" }, nil, false, h.opaque)
        sock:send(s)
        return
    end
    table.insert(self.queue, body)
    local i = #self.queue
    self.pending[i] = {
        sock = sock,
        opaque = h.opaque
    }
end

processors[DLEDGER_REQUEST_CODE.GET] = function(self, sock, addr, h, body)
    local data = self.queue[tonumber(h.extFields.index)]
    if data then
        local s = core.encode(RESPONSE_CODE.SUCCESS, {}, data, false, h.opaque)
        sock:send(s)
    else
        local s = core.encode(RESPONSE_CODE.SYSTEM_ERROR, { remark = "not found" }, nil, false, h.opaque)
        sock:send(s)
    end
end

function _M:start()
    local self = self
    if not self.master then
        return
    end
    ngx.timer.every(0.1, function()
        for i, pending in pairs(self.pending) do
            local resHeader, _, err = core.request(DLEDGER_REQUEST_CODE.PUSH,
                "127.0.0.1:10001",
                { index = i },
                self.queue[i], false)
            if resHeader and resHeader.code == RESPONSE_CODE.SUCCESS then
                pending.code = RESPONSE_CODE.SUCCESS
            else
                pending.code = RESPONSE_CODE.SYSTEM_ERROR
            end
        end
    end)
end

function _M:processRequest(sock, addr, h, body)
    local processor = processors[h.code]
    if not processor then
        local s = core.encode(RESPONSE_CODE.REQUEST_CODE_NOT_SUPPORTED, { remark = 'request code not supported' }, nil, false, h.opaque)
        sock:send(s)
        return
    end
    processor(self, sock, addr, h, body)
end

function _M:processHangingRequest(sock)
    for i, pending in pairs(self.pending) do
        if pending.sock == sock and pending.code then
            local s = core.encode(pending.code, { index = i }, '', false, pending.opaque)
            sock:send(s)
            self.pending[i] = nil
        end
    end
end

return _M
