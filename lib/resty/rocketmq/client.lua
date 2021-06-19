local bit = require("bit")
local cjson_safe = require("cjson.safe")
local core = require("resty.rocketmq.core")
local split = require("resty.rocketmq.utils").split
local unpack = unpack
local char = string.char
local concat = table.concat
local REQUEST_CODE = core.REQUEST_CODE

local _M = {}
_M.__index = _M
function _M.new(nameservers)
    if #nameservers == 0 then
        return nil, 'no nameserver'
    end
    local nameservers_parsed = {}
    for _, v in ipairs(nameservers) do

        local ip, port = unpack(split(v, ':'))
        if not port then
            return nil, 'nameserver no port:' .. v
        end
        table.insert(nameservers_parsed, {
            ip = ip,
            port = port
        })
    end
    return setmetatable({
        nameservers = nameservers_parsed,
        current_nameserver = 1,
        RPCHook = {},
        useTLS = false,
    }, _M)
end

function _M.addRPCHook(self, hook)
    if type(hook) == 'table' and type(hook.doBeforeRequest) == 'function' and type(hook.doAfterResponse) == 'function' then
        table.insert(self.RPCHook, hook)
    else
        return nil, 'hook should be functions'
    end
end

function _M.setUseTLS(self, useTLS)
    self.useTLS = useTLS
end

function _M:chooseNameserver()
    local nameserver = self.nameservers[self.current_nameserver]
    self.current_nameserver = self.current_nameserver + 1
    if self.current_nameserver > #self.nameservers then
        self.current_nameserver = 1
    end
    return nameserver
end

function _M:getTopicRouteInfoFromNameserver(topic)
    local nameserver = self:chooseNameserver()
    local addr = nameserver.ip .. ':' .. nameserver.port
    return core.request(REQUEST_CODE.GET_ROUTEINFO_BY_TOPIC, addr, { topic = topic }, nil, false, self.RPCHook, self.useTLS)
end

local function messageProperties2String(properties)
    local res = {}
    for k, v in pairs(properties) do
        table.insert(res, k .. char(1) .. v .. char(2))
    end
    return concat(res, '')
end

function _M:sendMessage(brokerAddr, msg)
    return core.request(REQUEST_CODE.SEND_MESSAGE_V2, brokerAddr, {
        a = msg.producerGroup,
        b = msg.topic,
        c = msg.defaultTopic,
        d = msg.defaultTopicQueueNums,
        e = msg.queueId,
        f = msg.sysFlag,
        g = msg.bornTimeStamp,
        h = msg.flag,
        i = messageProperties2String(msg.properties),
        j = msg.reconsumeTimes,
        k = msg.unitMode,
        l = msg.maxReconsumeTimes,
        m = msg.batch,
    }, msg.body, false, self.RPCHook, self.useTLS)
end

function _M:endTransactionOneway(brokerAddr, msg)
    return core.request(REQUEST_CODE.END_TRANSACTION, brokerAddr, msg, nil, true, self.RPCHook, self.useTLS)
end

function _M:sendHeartbeat(brokerAddr, heartbeatData)
    return core.request(REQUEST_CODE.HEART_BEAT, brokerAddr, {}, cjson_safe.encode(heartbeatData), false, self.RPCHook, self.useTLS)
end

return _M
