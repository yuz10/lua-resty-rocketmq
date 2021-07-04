local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local bit = require("bit")
local cjson_safe = require("cjson.safe")
local decode = require("resty.rocketmq.json").decode

local bor = bit.bor
local REQUEST_CODE = core.REQUEST_CODE
local RESPONSE_CODE = core.RESPONSE_CODE

local _M = {}
_M.__index = _M
function _M.new(nameservers)
    local cli, err = client.new(nameservers)
    if not cli then
        return nil, err
    end
    return setmetatable({
        client = cli,
    }, _M)
end

function _M.addRPCHook(self, hook)
    self.client:addRPCHook(hook)
end

function _M.setUseTLS(self, useTLS)
    self.client:setUseTLS(useTLS)
end

function _M.createTopic(self, defaultTopic, newTopic, queueNum, topicSysFlag)
    if not core.checkTopic(newTopic) then
        return nil, ('topic %s invalid format'):format(newTopic)
    end
    if core.isSystemTopic(newTopic) then
        return nil, ('topic %s is system topic'):format(newTopic)
    end
    local h, b, err = self.client:getTopicRouteInfoFromNameserver(defaultTopic)
    if not h then
        return nil, err
    end
    if h.code ~= core.RESPONSE_CODE.SUCCESS then
        return nil, ('getTopicRouteInfoFromNameserver return %s, %s'):format(core.RESPONSE_CODE_NAME[h.code] or h.code, h.remark or '')
    end
    local topicRouteData, err = decode(b)
    if not topicRouteData then
        return nil, err
    end

    local brokerDataList = topicRouteData.brokerDatas
    if brokerDataList == nil or #brokerDataList == 0 then
        return nil, 'Not found broker, maybe key is wrong'
    end

    local topicConfig = {
        topicName = newTopic,
        readQueueNums = queueNum,
        writeQueueNums = queueNum,
        topicSysFlag = topicSysFlag or 0,
    }

    local createOKAtLeastOnce = false
    local createErr
    for _, bd in ipairs(brokerDataList) do
        local addr = bd.brokerAddrs[0]
        if addr then
            local res, _
            res, _, createErr = _M.createTopicForBroker(self, addr, defaultTopic, topicConfig)
            if res and res.code == RESPONSE_CODE.SUCCESS then
                createOKAtLeastOnce = true
            else
                createErr = createErr or res.remark
            end
        end
    end
    if not createOKAtLeastOnce then
        return nil, createErr
    end
    return topicConfig
end

function _M.createTopicForBroker(self, addr, defaultTopic, topicConfig)
    return self.client:request(REQUEST_CODE.UPDATE_AND_CREATE_TOPIC, addr, {
        topic = topicConfig.topicName,
        defaultTopic = defaultTopic,
        readQueueNums = topicConfig.readQueueNums or 16,
        writeQueueNums = topicConfig.writeQueueNums or 16,
        perm = topicConfig.perm or bor(core.PERM_READ, core.PERM_WRITE),
        topicFilterType = topicConfig.topicFilterType or "SINGLE_TAG",
        topicSysFlag = topicConfig.topicSysFlag or 0,
        order = topicConfig.order or false,
    })
end

function _M.searchOffset(self, mq, timestamp)
    local brokerAddr = self.client:findBrokerAddressInPublish(mq.brokerName, mq.topic)
    if not brokerAddr then
        return nil, ("The broker[%s] not exist"):format(mq.brokerName)
    end
    local res, _, err = self.client:request(REQUEST_CODE.SEARCH_OFFSET_BY_TIMESTAMP, brokerAddr, {
        topic = mq.topic,
        queueId = mq.queueId,
        timestamp = timestamp,
    })
    if not res then
        return nil, err
    end
    local header = cjson_safe.decode(res)
    return header.offset
end

function _M.maxOffset(self, mq)
    local brokerAddr = self.client:findBrokerAddressInPublish(mq.brokerName, mq.topic)
    if not brokerAddr then
        return nil, ("The broker[%s] not exist"):format(mq.brokerName)
    end
    local res, _, err = self.client:request(REQUEST_CODE.GET_MAX_OFFSET, brokerAddr, {
        topic = mq.topic,
        queueId = mq.queueId,
    })
    if not res then
        return nil, err
    end
    return tonumber(res.extFields.offset)
end

function _M.minOffset(self, mq)
    local brokerAddr = self.client:findBrokerAddressInPublish(mq.brokerName, mq.topic)
    if not brokerAddr then
        return nil, ("The broker[%s] not exist"):format(mq.brokerName)
    end
    local res, _, err = self.client:request(REQUEST_CODE.GET_MIN_OFFSET, brokerAddr, {
        topic = mq.topic,
        queueId = mq.queueId,
    })
    if not res then
        return nil, err
    end
    return tonumber(res.extFields.offset)
end

function _M.earliestMsgStoreTime(self, mq)
    local brokerAddr = self.client:findBrokerAddressInPublish(mq.brokerName, mq.topic)
    if not brokerAddr then
        return nil, ("The broker[%s] not exist"):format(mq.brokerName)
    end
    local res, _, err = self.client:request(REQUEST_CODE.GET_EARLIEST_MSG_STORETIME, brokerAddr, {
        topic = mq.topic,
        queueId = mq.queueId,
    })
    if not res then
        return nil, err
    end
    return tonumber(res.extFields.timestamp)
end

function _M.viewMessage(self, offsetMsgId)
    local ok, addr, phyoffset = pcall(utils.decodeMessageId, offsetMsgId)
    if not ok then
        return nil, "query message by id finished, but no message."
    end
    local res, body, err = self.client:request(REQUEST_CODE.VIEW_MESSAGE_BY_ID, addr, {
        offset = phyoffset,
    })
    if not res then
        return nil, err
    end
    return core.decodeMsg(body, true, true)
end

return _M
