#!/usr/local/openresty/bin/resty -c1000

package.path = ';../lib/?.lua;' .. package.path

local core = require("resty.rocketmq.core")
local cjson = require("cjson")
local admin = require "resty.rocketmq.admin"
local utils = require("resty.rocketmq.utils")
local bit = require("bit")
local bor = bit.bor
local split = utils.split

local function fetchMasterAddrByClusterName(adm, clusterName)
    local clusterInfo, err = adm:examineBrokerClusterInfo()
    if not clusterInfo then
        print("get broker cluster info err:", err)
        return
    end
    local masterAddrs = {}
    for _, bname in ipairs(clusterInfo.clusterAddrTable[clusterName]) do
        local broker = clusterInfo.brokerAddrTable[bname]
        masterAddrs[broker.brokerAddrs[0]] = true
    end
    return utils.keys(masterAddrs)
end

local function getBrokerAddrs(adm, clusterName, brokerAddr)
    if clusterName then
        return fetchMasterAddrByClusterName(adm, clusterName)
    elseif brokerAddr then
        return { brokerAddr }
    else
        error('no broker or cluster')
    end
end

local cmds = {}
table.insert(cmds, { "updateTopic", function(adm, args)
    local clusterName = args['-c']
    local brokerAddr = args['-b']
    local order = args['-o']
    local perm = args['-p']
    local readQueueNum = args['-r']
    local writeQueueNum = args['-w']
    local topic = args['-t']
    for _, addr in ipairs(getBrokerAddrs(adm, clusterName, brokerAddr)) do
        local topicConf = {
            topicName = topic,
            readQueueNums = readQueueNum or 8,
            writeQueueNums = writeQueueNum or 8,
            perm = perm or bor(core.PERM_READ, core.PERM_WRITE),
            topicFilterType = "SINGLE_TAG",
            topicSysFlag = 0,
            order = order,
        }
        local res, err = adm:createTopicForBroker(addr, topicConf)
        if res then
            print(("create topic to %s success."):format(addr))
            print(cjson.encode(topicConf))
        else
            print("create topic err:", err)
            return
        end
    end
end })

table.insert(cmds, { "deleteTopic", function(adm, args)
    local clusterName = args['-c']
    local topic = args['-t']
    if clusterName == nil then
        print("no clusterName")
        return
    end
    local res, err = adm:deleteTopicInBroker(fetchMasterAddrByClusterName(adm, clusterName), topic)
    if not res then
        print("delete topic in broker err:", err)
        return
    end
    local res, err = adm:deleteTopicInNameServer(topic)
    if not res then
        print("delete topic in nameserver err:", err)
        return
    end
    print(("delete topic %s success."):format(topic))
end })

table.insert(cmds, { "topicStatus", function(adm, args)
    local topic = args['-t']
    local topicStatsTable, err = adm:examineTopicStats(topic)
    if not topicStatsTable then
        print("get topic stats err:", err)
        return
    end
    print(("%-32s  %-4s  %-20s  %-20s    %s"):format("#Broker Name", "#QID", "#Min Offset", "#Max Offset", "#Last Updated"))
    local mqList = utils.keys(topicStatsTable)
    table.sort(mqList)
    for _, mqKey in ipairs(mqList) do
        local mq = utils.buildMq(mqKey)
        local stats = topicStatsTable[mqKey]
        print(("%-32s  %-4d  %-20d  %-20d    %s"):format(
                mq.brokerName,
                mq.queueId,
                stats.minOffset,
                stats.maxOffset,
                stats.lastUpdateTimestamp == 0 and '' or utils.timeMillisToHumanString2(stats.lastUpdateTimestamp)
        ))
    end
end })

table.insert(cmds, { "topicRoute", function(adm, args)
    local topic = args['-t']
    local topicRouteData, err = adm.client:getTopicRouteInfoFromNameserver(topic)
    if not topicRouteData then
        print("get topic route err:", err)
        return
    end
    print("brokerDatas:")
    for _, bd in ipairs(topicRouteData.brokerDatas) do
        print('    ', cjson.encode(bd))
    end
    print("queueDatas:")
    for _, qd in ipairs(topicRouteData.queueDatas) do
        print('    ', cjson.encode(qd))
    end
end })

table.insert(cmds, { "topicList", function(adm, args)
    local clusterName = args['-c']
    local topics, err = adm:getTopicListFromNameServer()
    if not topics then
        print("get topic list err:", err)
        return
    end
    if clusterName then
        print(("%-20s  %-48s  %-48s"):format("#Cluster Name", "#Topic", "#Consumer Group"))
        local clusterInfo = adm:examineBrokerClusterInfo()
        print(cjson.encode(clusterInfo))
        for _, topic in ipairs(topics.topicList) do
            if not utils.startsWith(topic, core.RETRY_GROUP_TOPIC_PREFIX) then
                local topicRouteData = adm.client:getTopicRouteInfoFromNameserver(topic)
                local brokerName = topicRouteData.brokerDatas[1].brokerName
                local cluster = ''
                for clusterName, brokers in pairs(clusterInfo.clusterAddrTable) do
                    for _, bName in ipairs(brokers) do
                        if bName == brokerName then
                            cluster = clusterName
                            goto out
                        end
                    end
                end
                :: out ::
                local groupList = adm:queryTopicConsumeByWho(topic).groupList
                if groupList == nil or #groupList == 0 then
                    groupList = { "" }
                end
                for _, group in ipairs(groupList) do
                    print(("%-20s  %-48s  %-48s"):format(cluster, topic, group))
                end
            end
        end
    else
        for _, topic in ipairs(topics.topicList) do
            print(topic)
        end
    end
end })

table.insert(cmds, { "clusterList", function(adm, args)
    local clusterName = args['-c']
    local clusterInfo, err = adm:examineBrokerClusterInfo()
    if not clusterInfo then
        print("get broker cluster info err:", err)
        return
    end
    print(("%-22s  %-22s  %-4s  %-22s %-16s  %16s  %16s  %-22s  %-11s  %-12s  %-8s  %-10s"):format(
            "#Cluster Name", "#Broker Name", "#BID", "#Addr", "#Version", "#InTPS(LOAD)", "#OutTPS(LOAD)", "#Timer(Progress)", "#PCWait(ms)", "#Hour", "#SPACE", "#ACTIVATED"
    ))
    for cname, brokers in pairs(clusterInfo.clusterAddrTable) do
        if clusterName == nil or cname == clusterName then
            for _, bname in ipairs(brokers) do
                local broker = clusterInfo.brokerAddrTable[bname]
                for bid, baddr in pairs(broker.brokerAddrs) do
                    local kvTable, err = adm:fetchBrokerRuntimeStats(baddr);
                    if kvTable then
                        local t = kvTable.table
                        local hour = (ngx.now() - tonumber(t.earliestMessageTimeStamp) / 1000) / 3600
                        print(("%-22s  %-22s  %-4s  %-22s %-16s  %16s  %16s  %-22s  %11s  %-12s  %-8s  %10s"):format(
                                cname,
                                bname,
                                bid,
                                baddr,
                                kvTable.table.brokerVersionDesc,
                                ("%9.2f(%s,%sms)"):format(split(t.putTps, ' ')[1], t.sendThreadPoolQueueSize, t.sendThreadPoolQueueHeadWaitTimeMills),
                                ("%9.2f(%s,%sms)"):format(split(t.getTransferredTps or t.getTransferedTps, ' ')[1], t.pullThreadPoolQueueSize, t.pullThreadPoolQueueHeadWaitTimeMills),
                                ("%d-%d(%.1fw, %.1f, %.1f)"):format(t.timerReadBehind or 0, t.timerOffsetBehind or 0, tonumber(t.timerCongestNum or '0') / 10000, t.timerEnqueueTps or 0, t.timerDequeueTps or 0),
                                t.pageCacheLockTimeMills,
                                ("%2.2f"):format(hour),
                                t.commitLogDiskRatio,
                                t.brokerActive
                        ))
                    else
                        print(err)
                    end
                end
            end
        end
    end
end })

local function help()
    print("The most commonly used mqadmin commands are:")
    for _, cmd in ipairs(cmds) do
        print("    ", cmd[1])
    end
end

local res, err = pcall(function()
    local cmdName = string.lower(arg[1])
    local args = {}
    for i = 2, #arg, 2 do
        args[arg[i]] = arg[i + 1]
    end
    local ns = args['-n'] or "127.0.0.1:9876"
    local nameservers = split(ns, ';')
    local adm, err = admin.new(nameservers)
    if not adm then
        print("create admin err:", err)
        return
    end
    for _, cmd in ipairs(cmds) do
        if string.lower(cmd[1]) == cmdName then
            cmd[2](adm, args)
        end
    end
end)
if not res then
    print(err)
    help()
end


