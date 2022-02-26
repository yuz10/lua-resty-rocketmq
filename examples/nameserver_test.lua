#!/usr/local/openresty/bin/resty -c1000

package.path = ';../lib/?.lua;' .. package.path

local cjson = require("cjson")
local client = require "resty.rocketmq.client"
local core = require "resty.rocketmq.core"
local nameserver = require "resty.rocketmq.nameserver"
local REQUEST_CODE = core.REQUEST_CODE

local nameservers = { "127.0.0.1:9876" }
local p, err = client.new(nameservers)
if not p then
    print("create producer err:", err)
    return
end

local function realTest(code, h, body)
    p:request(code, "localhost:9876", h, body)
end

local n = nameserver.new().processor
local function mockTest(code, h, body)
    local res = n:processRequest(nil, { code = code, extFields = h }, body)
    print(cjson.encode(res))
end

mockTest(REQUEST_CODE.PUT_KV_CONFIG, { namespace = "", key = "12", value = "123" })
mockTest(REQUEST_CODE.DELETE_KV_CONFIG, { namespace = "", key = "12" })
mockTest(REQUEST_CODE.GET_KV_CONFIG, { namespace = "", key = "12" })
mockTest(REQUEST_CODE.QUERY_DATA_VERSION, { brokerAddr = "localhost:1234" },
        cjson.encode { timestamp = ngx.now() * 1000, counter = 1 })

local body = '{"filterServerList":[],"topicConfigSerializeWrapper":{"dataVersion":{"counter":293,"timestamp":1645882625002},"topicConfigTable":{"SCHEDULE_TOPIC_XXXX":{"order":false,"perm":6,"readQueueNums":18,"topicFilterType":"SINGLE_TAG","topicName":"SCHEDULE_TOPIC_XXXX","topicSysFlag":0,"writeQueueNums":18},"TopicTest":{"order":false,"perm":6,"readQueueNums":8,"topicFilterType":"SINGLE_TAG","topicName":"TopicTest","topicSysFlag":0,"writeQueueNums":8},"%DLQ%CID_JODIE":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"%DLQ%CID_JODIE","topicSysFlag":0,"writeQueueNums":1},"SELF_TEST_TOPIC":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"SELF_TEST_TOPIC","topicSysFlag":0,"writeQueueNums":1},"%RETRY%CID_JODIE_1":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"%RETRY%CID_JODIE_1","topicSysFlag":0,"writeQueueNums":1},"DefaultCluster":{"order":false,"perm":7,"readQueueNums":16,"topicFilterType":"SINGLE_TAG","topicName":"DefaultCluster","topicSysFlag":0,"writeQueueNums":16},"DefaultCluster_REPLY_TOPIC":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"DefaultCluster_REPLY_TOPIC","topicSysFlag":0,"writeQueueNums":1},"%RETRY%TOOLS_CONSUMER":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"%RETRY%TOOLS_CONSUMER","topicSysFlag":0,"writeQueueNums":1},"RMQ_SYS_TRANS_HALF_TOPIC":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"RMQ_SYS_TRANS_HALF_TOPIC","topicSysFlag":0,"writeQueueNums":1},"TRANS_CHECK_MAX_TIME_TOPIC":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"TRANS_CHECK_MAX_TIME_TOPIC","topicSysFlag":0,"writeQueueNums":1},"broker-a":{"order":false,"perm":7,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"broker-a","topicSysFlag":0,"writeQueueNums":1},"RMQ_SYS_TRACE_TOPIC":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"RMQ_SYS_TRACE_TOPIC","topicSysFlag":0,"writeQueueNums":1},"TBW102":{"order":false,"perm":7,"readQueueNums":8,"topicFilterType":"SINGLE_TAG","topicName":"TBW102","topicSysFlag":0,"writeQueueNums":8},"%RETRY%CID_JODIE":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"%RETRY%CID_JODIE","topicSysFlag":0,"writeQueueNums":1},"BenchmarkTest":{"order":false,"perm":6,"readQueueNums":1024,"topicFilterType":"SINGLE_TAG","topicName":"BenchmarkTest","topicSysFlag":0,"writeQueueNums":1024},"OFFSET_MOVED_EVENT":{"order":false,"perm":6,"readQueueNums":1,"topicFilterType":"SINGLE_TAG","topicName":"OFFSET_MOVED_EVENT","topicSysFlag":0,"writeQueueNums":1}}}}'
mockTest(REQUEST_CODE.REGISTER_BROKER, {
    brokerName = "broker-a",
    compressed = "false",
    brokerId = "0",
    clusterName = "DefaultCluster",
    bodyCrc32 = "1794280071",
    brokerAddr = "172.21.156.94:10911",
    haServerAddr = "172.21.156.95:10912"
}, body)

mockTest(REQUEST_CODE.REGISTER_BROKER, {
    brokerName = "broker-a",
    compressed = "false",
    brokerId = "1",
    clusterName = "DefaultCluster",
    bodyCrc32 = "1794280071",
    brokerAddr = "172.21.156.95:10911",
    haServerAddr = "172.21.156.94:10912"
}, body)

mockTest(REQUEST_CODE.UNREGISTER_BROKER, {
    brokerName = "broker-a",
    brokerId = "1",
    clusterName = "DefaultCluster",
    brokerAddr = "172.21.156.95:10911",
})

mockTest(REQUEST_CODE.GET_ROUTEINFO_BY_TOPIC, { topic = "TopicTest", })
mockTest(REQUEST_CODE.GET_BROKER_CLUSTER_INFO, { topic = "TopicTest", })
mockTest(REQUEST_CODE.WIPE_WRITE_PERM_OF_BROKER, { brokerName = "broker-a", })
mockTest(REQUEST_CODE.ADD_WRITE_PERM_OF_BROKER, { brokerName = "broker-a", })
mockTest(REQUEST_CODE.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, {})
mockTest(REQUEST_CODE.DELETE_TOPIC_IN_NAMESRV, { topic = "TopicTest" })
mockTest(REQUEST_CODE.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, {})
mockTest(REQUEST_CODE.GET_KVLIST_BY_NAMESPACE, { namespace = core.ORDER_TOPIC_CONFIG })
mockTest(REQUEST_CODE.GET_TOPICS_BY_CLUSTER, { cluster = "DefaultCluster" })
realTest(REQUEST_CODE.GET_SYSTEM_TOPIC_LIST_FROM_NS, {})


