#!/usr/local/openresty/bin/resty -c1000

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local admin = require "resty.rocketmq.admin"
local utils = require("resty.rocketmq.utils")
local split = utils.split

local cmds = {}
table.insert(cmds, { "clusterList", function(args)
    local ns = args['-n'] or "127.0.0.1:9876"
    local clusterName = args['-c']
    local nameservers = split(ns, ';')
    local adm, err = admin.new(nameservers)
    if not adm then
        print("create admin err:", err)
        return
    end
    local clusterInfo, err = adm:getBrokerClusterInfo()
    if not clusterInfo then
        print("get broker cluster info err:", err)
        return
    end
    print(("%-22s  %-22s  %-4s  %-22s %-16s  %16s  %16s  %-22s  %-11s  %-12s  %-8s  %-10s"):format(
            "#Cluster Name",
            "#Broker Name",
            "#BID",
            "#Addr",
            "#Version",
            "#InTPS(LOAD)",
            "#OutTPS(LOAD)",
            "#Timer(Progress)",
            "#PCWait(ms)",
            "#Hour",
            "#SPACE",
            "#ACTIVATED"
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
    for _, cmd in ipairs(cmds) do
        if string.lower(cmd[1]) == cmdName then
            cmd[2](args)
        end
    end
end)
if not res then
    print(err)
    help()
end


