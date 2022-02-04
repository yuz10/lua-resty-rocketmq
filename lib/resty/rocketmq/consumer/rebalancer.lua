local core = require("resty.rocketmq.core")
local utils = require("resty.rocketmq.utils")
local trace = require("resty.rocketmq.trace")
local RESPONSE_CODE = core.RESPONSE_CODE
local cjson_safe = require("cjson.safe")
local ngx = ngx
local ngx_timer_at = ngx.timer.at
local log = ngx.log
local ERR = ngx.ERR

---@class rebalancer
local _M = {}
_M.__index = _M

function _M.new(client, consumerGroup, allocateMessageQueueStrategy)
    local consumer = setmetatable({
        consumerGroup = consumerGroup,
        client = client,
        allocateMessageQueueStrategy = allocateMessageQueueStrategy,
        topicSubscribeInfoTable = {},
        subscriptionInner = {},
    }, _M)
    return consumer
end

local function rebalanceByTopic(self, topic)
    local mqSet = self.client.topicSubscribeInfoTable[topic]
    local cidAll = findConsumerIdList(topic, self.consumerGroup);


end

local function doRebalance(self)
    for topic, _ in pairs(self.subscriptionInner) do
        rebalanceByTopic(self, topic)
    end
end

function _M:start()
    local self = self
    local loop
    loop = function()
        if self.exit then
            return
        end
        doRebalance()
        ngx_timer_at(20, loop)
    end
    ngx_timer_at(20, loop)
end

return _M