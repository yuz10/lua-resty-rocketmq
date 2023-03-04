local core = require("resty.rocketmq.core")
local client = require("resty.rocketmq.client")
local utils = require("resty.rocketmq.utils")
local decoderFromTraceDataString = require("resty.rocketmq.trace").decoderFromTraceDataString
local bit = require("bit")
local cjson_safe = require("cjson.safe")
local split = require("resty.rocketmq.utils").split
local decode = require("resty.rocketmq.json").decode

local bor = bit.bor
local REQUEST_CODE = core.REQUEST_CODE
local RESPONSE_CODE = core.RESPONSE_CODE
local ngx = ngx
local log = ngx.log
local WARN = ngx.WARN
local ngx_thread_spawn = ngx.thread.spawn
local ngx_thread_wait = ngx.thread.wait

local _M = {}
_M.__index = _M
function _M.new(nameservers, cli)
    if not cli then
        local err
        cli, err = client.new(nameservers)
        if not cli then
            return nil, err
        end
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

function _M.setTimeout(self, timeout)
    self.client:setTimeout(timeout)
end

function _M.examineBrokerClusterInfo(self)
    local addr = self.client:chooseNameserver()
    local res, body, err = self.client:request(REQUEST_CODE.GET_BROKER_CLUSTER_INFO, addr, {})
    if not res then
        return nil, err
    end
    if res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
    end
    return decode(body)
end

function _M.fetchBrokerRuntimeStats(self, brokerAddr)
    local res, body, err = self.client:request(REQUEST_CODE.GET_BROKER_RUNTIME_INFO, brokerAddr, {})
    if not res then
        return nil, err
    end
    if res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
    end
    return cjson_safe.decode(body)
end

function _M.createTopic(self, newTopic, queueNum, topicSysFlag)
    if not core.checkTopic(newTopic) then
        return nil, ('topic %s invalid format'):format(newTopic)
    end
    if core.isSystemTopic(newTopic) then
        return nil, ('topic %s is system topic'):format(newTopic)
    end
    local topicRouteData, err = self.client:getTopicRouteInfoFromNameserver("TBW102")
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
            res, createErr = _M.createTopicForBroker(self, addr, topicConfig)
            if res then
                createOKAtLeastOnce = true
            end
        end
    end
    if not createOKAtLeastOnce then
        return nil, createErr
    end
    return topicConfig
end

function _M.createTopicForBroker(self, addr, topicConfig)
    local res, _, err = self.client:request(REQUEST_CODE.UPDATE_AND_CREATE_TOPIC, addr, {
        topic = topicConfig.topicName,
        defaultTopic = "TBW102",
        readQueueNums = topicConfig.readQueueNums or 16,
        writeQueueNums = topicConfig.writeQueueNums or 16,
        perm = topicConfig.perm or bor(core.PERM_READ, core.PERM_WRITE),
        topicFilterType = topicConfig.topicFilterType or "SINGLE_TAG",
        topicSysFlag = topicConfig.topicSysFlag or 0,
        order = topicConfig.order or false,
    })
    if res and res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
    end
    return res, err
end

function _M.deleteTopicInBroker(self, addrs, topic)
    for _, addr in ipairs(addrs) do
        local res, _, err = self.client:request(REQUEST_CODE.DELETE_TOPIC_IN_BROKER, addr, {
            topic = topic,
        })
        if res and res.code ~= RESPONSE_CODE.SUCCESS then
            return nil, res.remark
        end
        return res, err
    end
end

function _M.deleteTopicInNameServer(self, topic)
    for _, addr in ipairs(self.client.nameservers) do
        local res, _, err = self.client:request(REQUEST_CODE.DELETE_TOPIC_IN_NAMESRV, addr, {
            topic = topic,
        })
        if res and res.code ~= RESPONSE_CODE.SUCCESS then
            return nil, res.remark
        end
        return res, err
    end
end

function _M.getTopicStatsInfo(self, addr, topic)
    local res, body, err = self.client:request(REQUEST_CODE.GET_TOPIC_STATS_INFO, addr, {
        topic = topic,
    })
    if res and res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
    end
    return decode(body)
end

function _M.examineTopicStats(self, topic)
    local topicRouteData, err = self.client:getTopicRouteInfoFromNameserver(topic)
    if not topicRouteData then
        return nil, err
    end
    local offsetTable = {}
    for _, bd in ipairs(topicRouteData.brokerDatas) do
        local brokerAddr = bd.brokerAddrs[0]
        if brokerAddr then
            local topicStatsTable, err = self:getTopicStatsInfo(brokerAddr, topic)
            if not topicStatsTable then
                return nil, err
            end
            for k, v in pairs(topicStatsTable.offsetTable) do
                offsetTable[utils.buildMqKey(k)] = v
            end
        end
    end
    return offsetTable
end

function _M.getTopicListFromNameServer(self)
    local addr = self.client:chooseNameserver()
    local res, body, err = self.client:request(REQUEST_CODE.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, addr, {})
    if not res then
        return nil, err
    end
    if res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
    end
    return cjson_safe.decode(body)
end

function _M.queryTopicConsumeByWho(self, topic)
    local topicRouteData = self.client:getTopicRouteInfoFromNameserver(topic)
    for _, bd in ipairs(topicRouteData.brokerDatas) do
        local res, body, err = self.client:request(REQUEST_CODE.QUERY_TOPIC_CONSUME_BY_WHO, bd.brokerAddrs[0], {
            topic = topic
        })
        if not res then
            return nil, err
        end
        if res.code ~= RESPONSE_CODE.SUCCESS then
            return nil, res.remark
        end
        return cjson_safe.decode(body)
    end
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
    if res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
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
        bname = mq.brokerName,
    })
    if not res then
        return nil, err
    end
    if res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
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
    if res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
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
    if res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
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
    if res.code ~= RESPONSE_CODE.SUCCESS then
        return nil, res.remark
    end
    return core.decodeMsg(body, true, true)
end

function _M.queryMessage(self, topic, key, maxNum, beginTime, endTime, isUniqKey)
    local publishInfo, err = self.client:tryToFindTopicPublishInfo(topic)
    if err then
        return nil, err
    end
    local topicRouteData = publishInfo.topicRouteData
    local allBrokerAddrs = {}
    for _, brokerDatas in ipairs(topicRouteData.brokerDatas) do
        local brokerAddrs = brokerDatas.brokerAddrs
        local addr = brokerAddrs[0] or next(brokerAddrs)
        if addr then
            table.insert(allBrokerAddrs, addr)
        end
    end
    if #allBrokerAddrs == 0 then
        return nil, "The topic[" .. topic .. "] not matched route info"
    end
    local threads = {}
    for i, brokerAddr in ipairs(allBrokerAddrs) do
        threads[i] = ngx_thread_spawn(function()
            return self.client:request(REQUEST_CODE.QUERY_MESSAGE, brokerAddr, {
                topic = topic,
                key = key,
                maxNum = maxNum,
                beginTimestamp = beginTime,
                endTimestamp = endTime,
                _UNIQUE_KEY_QUERY = tostring(isUniqKey)
            })
        end, brokerAddr)
    end
    local msgs = {}
    for i, thread in ipairs(threads) do
        local ok, res, body, err = ngx_thread_wait(thread)
        if not ok then
            log(WARN, allBrokerAddrs[i], ' return ', res)
        elseif not res then
            log(WARN, allBrokerAddrs[i], ' return ', err)
        elseif res.code == RESPONSE_CODE.SUCCESS then
            core.decodeMsgs(msgs, body, true, true)
        else
            log(WARN, allBrokerAddrs[i], ' return ', core.RESPONSE_CODE_NAME[res.code], ' remark:', core.remark)
        end
    end
    local msgs_filter = {}
    for _, msg in ipairs(msgs) do
        if msg.topic == topic then
            if isUniqKey then
                if msg.properties.UNIQ_KEY == key then
                    table.insert(msgs_filter, msg)
                end
            else
                local matched = false
                for _, k in ipairs(split(msg.properties.KEYS, ' ')) do
                    if k == key then
                        matched = true
                        break
                    end
                end
                if matched then
                    table.insert(msgs_filter, msg)
                end
            end
        end
    end
    return msgs_filter
end

function _M.queryTraceByMsgId(self, traceTopic, msgId)
    local msgs, err = self:queryMessage(traceTopic or core.RMQ_SYS_TRACE_TOPIC, msgId, 64, 0, (ngx.now() + 1) * 1000)
    if not msgs then
        return nil, err
    end
    local trace = {}
    for _, msg in ipairs(msgs) do
        local ctxList = decoderFromTraceDataString(msg)
        for _, ctx in ipairs(ctxList) do
            if ctx.msgId == msgId then
                table.insert(trace, ctx)
            end
        end
    end
    return trace
end

return _M
