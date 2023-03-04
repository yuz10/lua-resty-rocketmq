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

local function fetchMasterAndSlaveAddrByClusterName(adm, clusterName)
    local clusterInfo, err = adm:examineBrokerClusterInfo()
    if not clusterInfo then
        print("get broker cluster info err:", err)
        return
    end
    local addrs = {}
    for _, bname in ipairs(clusterInfo.clusterAddrTable[clusterName]) do
        local broker = clusterInfo.brokerAddrTable[bname]
        for _, addr in pairs(broker.brokerAddrs) do
            addrs[addr] = true
        end
    end
    return utils.keys(addrs)
end

local function getMasterAddrs(adm, clusterName, brokerAddr)
    if clusterName then
        return fetchMasterAddrByClusterName(adm, clusterName)
    elseif brokerAddr then
        return { brokerAddr }
    else
        error('no broker or cluster')
    end
end

local function printMsg(msg)
    print(("%-20s %s"):format("OffsetID:", msg.offsetMsgId))
    print(("%-20s %s"):format("Topic:", msg.topic))
    print(("%-20s [%s]"):format("Tags:", msg.properties.TAGS))
    print(("%-20s [%s]"):format("Keys:", msg.properties.KEYS))
    print(("%-20s %s"):format("Queue ID:", msg.queueId))
    print(("%-20s %s"):format("Queue Offset:", msg.queueOffset))
    print(("%-20s %s"):format("CommitLog Offset:", msg.commitLogOffset))
    print(("%-20s %s"):format("Reconsume Times:", msg.reconsumeTimes))
    print(("%-20s %s"):format("Born Timestamp:", utils.timeMillisToHumanString2(msg.bornTimeStamp)))
    print(("%-20s %s"):format("Store Timestamp:", utils.timeMillisToHumanString2(msg.storeTimestamp)))
    print(("%-20s %s"):format("Born Host:", msg.bornHost))
    print(("%-20s %s"):format("Store Host:", msg.storeHost))
    print(("%-20s %s"):format("System Flag:", msg.sysFlag))
    print(("%-20s %s"):format("Properties:", cjson.encode(msg.properties)))
    print(("%-20s %s"):format("Message Body:", msg.body))
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
    for _, addr in ipairs(getMasterAddrs(adm, clusterName, brokerAddr)) do
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
    return true
end, [[usage: mqadmin updateTopic [-a <arg>] -b <arg> | -c <arg>  [-h] [-n <arg>] [-o <arg>] [-p <arg>] [-r <arg>]
       [-s <arg>] -t <arg> [-u <arg>] [-w <arg>]
 -a,--attributes <arg>       attribute(+a=b,+c=d,-e)
 -b,--brokerAddr <arg>       create topic to which broker
 -c,--clusterName <arg>      create topic to which cluster
 -h,--help                   Print help
 -n,--namesrvAddr <arg>      Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -o,--order <arg>            set topic's order(true|false)
 -p,--perm <arg>             set topic's permission(2|4|6), intro[2:W 4:R; 6:RW]
 -r,--readQueueNums <arg>    set read queue nums
 -s,--hasUnitSub <arg>       has unit sub (true|false)
 -t,--topic <arg>            topic name
 -u,--unit <arg>             is unit topic (true|false)
 -w,--writeQueueNums <arg>   set write queue nums]] })

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
    return true
end, [[usage: mqadmin deleteTopic -c <arg> [-h] [-n <arg>] -t <arg>
 -c,--clusterName <arg>   delete topic from which cluster
 -h,--help                Print help
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -t,--topic <arg>         topic name]] })

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
    return true
end, [[usage: mqadmin topicStatus [-h] [-n <arg>] -t <arg>
 -h,--help                Print help
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -t,--topic <arg>         topic name]] })

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
    return true
end, [[usage: mqadmin topicRoute [-h] [-l] [-n <arg>] -t <arg>
 -h,--help                Print help
 -l,--list                Use list format to print data
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -t,--topic <arg>         topic name]] })

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
    return true
end, [[usage: mqadmin topicList [-c] [-h] [-n <arg>]
 -c,--clusterModel        clusterModel
 -h,--help                Print help
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876']] })

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
    return true
end, [[usage: mqadmin clusterList [-c <arg>] [-h] [-i <arg>] [-m] [-n <arg>]
 -c,--clusterName <arg>   which cluster
 -h,--help                Print help
 -i,--interval <arg>      specify intervals numbers, it is in seconds
 -m,--moreStats           Print more stats
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876']] })

table.insert(cmds, { "brokerStatus", function(adm, args)
    local function printBrokerRuntimeStats(brokerAddr, printBroker)
        local kvTable, err = adm:fetchBrokerRuntimeStats(brokerAddr)
        if err then
            print("get broker runtime stats error, ", err)
            return
        end
        for k, v in pairs(kvTable.table) do
            if printBroker then
                print(("%-24s %-32s: %s"):format(brokerAddr, k, v))
            else
                print(("%-32s: %s"):format(k, v))
            end
        end
    end
    local clusterName = args['-c']
    local brokerAddr = args['-b']
    if brokerAddr then
        printBrokerRuntimeStats(brokerAddr, false)
    else
        local addrs = fetchMasterAndSlaveAddrByClusterName(adm, clusterName)
        for _, addr in ipairs(addrs) do
            printBrokerRuntimeStats(addr, true)
        end
    end
    return true
end, [[usage: mqadmin brokerStatus -b <arg> | -c <arg>  [-h] [-n <arg>]
 -b,--brokerAddr <arg>    Broker address
 -c,--clusterName <arg>   which cluster
 -h,--help                Print help
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876']] })

table.insert(cmds, { "queryMsgById", function(adm, args)
    local msgIds = split(args['-i'], ',')
    if #msgIds == 0 then
        return
    end
    for _, msgId in ipairs(msgIds) do
        local msg, err = adm:viewMessage(msgId)
        if not msg then
            print('query msg fail:', err)
        else
            printMsg(msg)
        end
    end
    return true
end, [[usage: mqadmin queryMsgById [-d <arg>] [-f <arg>] [-g <arg>] [-h] -i <arg> [-n <arg>] [-s <arg>] [-u <arg>]
 -d,--clientId <arg>        The consumer's client id
 -f,--bodyFormat <arg>      print message body by the specified format
 -g,--consumerGroup <arg>   consumer group name
 -h,--help                  Print help
 -i,--msgId <arg>           Message Id
 -n,--namesrvAddr <arg>     Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -s,--sendMessage <arg>     resend message
 -u,--unitName <arg>        unit name]] })

table.insert(cmds, { "queryMsgByKey", function(adm, args)
    local topic = args['-t']
    local key = args['-k']
    local msgs, err = adm:queryMessage(topic, key, 64, 0, (ngx.now() + 10) * 1000, false)
    if not msgs then
        print('query msg fail:', err)
        return
    end
    for _, msg in ipairs(msgs) do
        printMsg(msg)
    end
    return true
end, [[usage: mqadmin queryMsgByKey [-h] -k <arg> [-n <arg>] -t <arg>
 -h,--help                Print help
 -k,--msgKey <arg>        Message Key
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -t,--topic <arg>         topic name]] })


table.insert(cmds, { "queryMsgByUniqueKey", function(adm, args)
    local topic = args['-t']
    local msgId = args['-i']
    local msgs, err = adm:queryMessage(topic, msgId, 64, 0, (ngx.now() + 10) * 1000, true)
    if not msgs then
        print('query msg fail:', err)
        return
    end
    for _, msg in ipairs(msgs) do
        printMsg(msg)
    end
    return true
end, [[usage: mqadmin queryMsgByUniqueKey [-a] [-d <arg>] [-g <arg>] [-h] -i <arg> [-n <arg>] -t <arg>
 -a,--showAll               Print all message, the limit is 32
 -d,--clientId <arg>        The consumer's client id
 -g,--consumerGroup <arg>   consumer group name
 -h,--help                  Print help
 -i,--msgId <arg>           Message Id
 -n,--namesrvAddr <arg>     Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -t,--topic <arg>           The topic of msg]] })


table.insert(cmds, { "queryMsgByOffset", function(adm, args)
    local topic = args['-t']
    local brokerName = args['-b']
    local queueId = args['-i']
    local offset = args['-o']
    local pullResult, err = adm.client:pullKernelImpl(brokerName, {
        consumerGroup = "TOOLS_CONSUMER",
        topic = topic,
        queueId = queueId,
        queueOffset = offset,
        maxMsgNums = 1,
        sysFlag = utils.buildSysFlag(false, false, true, false),
        commitOffset = 0,
        suspendTimeoutMillis = 1000,
        subscription = "*",
        subVersion = 0,
        expressionType = "TAG"
    }, 2000)
    if not pullResult then
        print("send err:", err)
        return
    end
    for _, msg in ipairs(pullResult.msgFoundList) do
        printMsg(msg)
    end
    return true
end, [[usage: mqadmin queryMsgByOffset -b <arg> [-h] -i <arg> [-n <arg>] -o <arg> -t <arg>
 -b,--brokerName <arg>    Broker Name
 -h,--help                Print help
 -i,--queueId <arg>       Queue Id
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -o,--offset <arg>        Queue Offset
 -t,--topic <arg>         topic name]] })

table.insert(cmds, { "queryMsgTraceById", function(adm, args)
    local msgId = args['-i']
    local traceTopic = args['-t'] or "RMQ_SYS_TRACE_TOPIC"
    local traces, err = adm:queryTraceByMsgId(traceTopic, msgId)
    if not traces then
        print('query msg fail:', err)
        return
    end
    for _, trace in ipairs(traces) do
        print(cjson.encode(trace))
    end
    return true
end, [[usage: mqadmin queryMsgTraceById [-h] -i <arg> [-n <arg>] [-t <arg>]
 -h,--help                Print help
 -i,--msgId <arg>         Message Id
 -n,--namesrvAddr <arg>   Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'
 -t,--traceTopic <arg>    The name value of message trace topic]] })

local function help()
    print("The most commonly used mqadmin commands are:")
    for _, cmd in ipairs(cmds) do
        print("    ", cmd[1])
    end
end

local cmdName, args
local res, err = pcall(function()
    cmdName = string.lower(arg[1])
    args = {}
    for i = 2, #arg, 2 do
        args[arg[i]] = arg[i + 1]
    end
end)
if not res then
    print(err)
    help()
    return
end

local ns = args['-n'] or "127.0.0.1:9876"
local nameservers = split(ns, ';')
local adm, err = admin.new(nameservers)
if not adm then
    print("create admin err:", err)
    return
end

local cmd
for _, c in ipairs(cmds) do
    if string.lower(c[1]) == cmdName then
        cmd = c
        break
    end
end

if not cmd then
    help()
    return
end

if args['-h'] then
    if cmd[3] then
        print(cmd[3])
    end
    return
end

local result
local res, err = pcall(function()
    result = cmd[2](adm, args)
end)
if not res or not result then
    print(err)
    if cmd[3] then
        print(cmd[3])
    end
end


