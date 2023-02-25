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
    local clusterInfo, err = adm:getBrokerClusterInfo()
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
        return fetchMasterAddrByClusterName(adm, clusterName);
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

table.insert(cmds, { "clusterList", function(adm, args)
    local clusterName = args['-c']
    local clusterInfo, err = adm:getBrokerClusterInfo()
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


