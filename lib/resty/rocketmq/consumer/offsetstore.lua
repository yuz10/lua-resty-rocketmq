local utils = require("resty.rocketmq.utils")

---@class offsetStore
local _M = {}
_M.__index = _M
_M.READ_FROM_MEMORY = 'READ_FROM_MEMORY'
_M.READ_FROM_STORE = 'READ_FROM_STORE'
_M.MEMORY_FIRST_THEN_STORE = 'MEMORY_FIRST_THEN_STORE'

function _M.new(consumer)
    ---@type offsetStore
    local offsetStore = setmetatable({
        consumer = consumer,
        client = consumer.client,
        offsetTable = {},
    }, _M)
    return offsetStore
end

function _M:updateOffset(mq, offset, increaseOnly)
    local mqKey = utils.buildMqKey(mq)
    local old = self.offsetTable[mqKey]
    if old and increaseOnly and offset < old then
        return nil, 'increaseOnly'
    end
    self.offsetTable[mqKey] = offset
end

local function readOffsetFromBroker(self, mq)
    local brokerOffset = self.client:fetchConsumeOffsetFromBroker(self.consumer.consumerGroup, mq);
    self:updateOffset(mq, brokerOffset, false)
    return brokerOffset
end

function _M:readOffset(mq, type)
    local mqKey = utils.buildMqKey(mq)
    if type == _M.READ_FROM_STORE then
        return readOffsetFromBroker(self, mq)
    end
    local offset = self.offsetTable[mqKey]
    if offset then
        return offset
    end
    if type == _M.MEMORY_FIRST_THEN_STORE then
        return readOffsetFromBroker(self, mq)
    else
        return -1
    end
end

function _M:persistAll(mqs)
    error("not implemented")
end

function _M:persist(mq)
    local mqKey = utils.buildMqKey(mq)
    local offset = self.offsetTable[mqKey]
    if offset then
        self.client:updateConsumeOffsetToBroker(mq, offset)
    end
end

function _M:removeOffset(mq)
    local mqKey = utils.buildMqKey(mq)
    self.offsetTable[mqKey] = nil
end
return _M
