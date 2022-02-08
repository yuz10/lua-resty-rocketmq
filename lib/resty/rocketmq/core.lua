local cjson_safe = require("cjson.safe")
local utils = require("resty.rocketmq.utils")
local bit = require("bit")
local rshift = bit.rshift
local band = bit.band
local bor = bit.bor
local lshift = bit.lshift
local concat = table.concat
local insert = table.insert
local char = string.char
local unpack = unpack
local byte = string.byte
local split = utils.split
local ngx = ngx
local ngx_socket_tcp = ngx.socket.tcp

local _M = {}
_M.serializeTypeCurrentRPC = "ROCKETMQ"  -- "JSON" or "ROCKETMQ"
local serializeTypeJson = 0
local serializeTypeRocketmq = 1

_M.CID_RMQ_SYS_PREFIX = 'CID_RMQ_SYS_'
_M.RETRY_GROUP_TOPIC_PREFIX = '%RETRY%'
_M.DLQ_GROUP_TOPIC_PREFIX = '%DLQ%'

local SYSTEM_TOPIC_PREFIX = 'rmq_sys_'
local SYSTEM_TOPIC_SET = {
    TBW102 = true,
    SCHEDULE_TOPIC_XXXX = true,
    BenchmarkTest = true,
    RMQ_SYS_TRANS_HALF_TOPIC = true,
    RMQ_SYS_TRACE_TOPIC = true,
    RMQ_SYS_TRANS_OP_HALF_TOPIC = true,
    TRANS_CHECK_MAX_TIME_TOPIC = true,
    SELF_TEST_TOPIC = true,
    OFFSET_MOVED_EVENT = true,
}
_M.RMQ_SYS_TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC"

local REQUEST_CODE = {
    SEND_MESSAGE = 10,
    PULL_MESSAGE = 11,
    QUERY_MESSAGE = 12,
    QUERY_BROKER_OFFSET = 13,
    QUERY_CONSUMER_OFFSET = 14,
    UPDATE_CONSUMER_OFFSET = 15,
    UPDATE_AND_CREATE_TOPIC = 17,
    GET_ALL_TOPIC_CONFIG = 21,
    GET_TOPIC_CONFIG_LIST = 22,
    GET_TOPIC_NAME_LIST = 23,
    UPDATE_BROKER_CONFIG = 25,
    GET_BROKER_CONFIG = 26,
    TRIGGER_DELETE_FILES = 27,
    GET_BROKER_RUNTIME_INFO = 28,
    SEARCH_OFFSET_BY_TIMESTAMP = 29,
    GET_MAX_OFFSET = 30,
    GET_MIN_OFFSET = 31,
    GET_EARLIEST_MSG_STORETIME = 32,
    VIEW_MESSAGE_BY_ID = 33,
    HEART_BEAT = 34,
    UNREGISTER_CLIENT = 35,
    CONSUMER_SEND_MSG_BACK = 36,
    END_TRANSACTION = 37,
    GET_CONSUMER_LIST_BY_GROUP = 38,
    CHECK_TRANSACTION_STATE = 39,
    NOTIFY_CONSUMER_IDS_CHANGED = 40,
    LOCK_BATCH_MQ = 41,
    UNLOCK_BATCH_MQ = 42,
    GET_ALL_CONSUMER_OFFSET = 43,
    GET_ALL_DELAY_OFFSET = 45,
    CHECK_CLIENT_CONFIG = 46,
    UPDATE_AND_CREATE_ACL_CONFIG = 50,
    DELETE_ACL_CONFIG = 51,
    GET_BROKER_CLUSTER_ACL_INFO = 52,
    UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53,
    GET_BROKER_CLUSTER_ACL_CONFIG = 54,
    PUT_KV_CONFIG = 100,
    GET_KV_CONFIG = 101,
    DELETE_KV_CONFIG = 102,
    REGISTER_BROKER = 103,
    UNREGISTER_BROKER = 104,
    GET_ROUTEINFO_BY_TOPIC = 105,
    GET_BROKER_CLUSTER_INFO = 106,
    UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200,
    GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201,
    GET_TOPIC_STATS_INFO = 202,
    GET_CONSUMER_CONNECTION_LIST = 203,
    GET_PRODUCER_CONNECTION_LIST = 204,
    WIPE_WRITE_PERM_OF_BROKER = 205,
    GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206,
    DELETE_SUBSCRIPTIONGROUP = 207,
    GET_CONSUME_STATS = 208,
    SUSPEND_CONSUMER = 209,
    RESUME_CONSUMER = 210,
    RESET_CONSUMER_OFFSET_IN_CONSUMER = 211,
    RESET_CONSUMER_OFFSET_IN_BROKER = 212,
    ADJUST_CONSUMER_THREAD_POOL = 213,
    WHO_CONSUME_THE_MESSAGE = 214,
    DELETE_TOPIC_IN_BROKER = 215,
    DELETE_TOPIC_IN_NAMESRV = 216,
    GET_KVLIST_BY_NAMESPACE = 219,
    RESET_CONSUMER_CLIENT_OFFSET = 220,
    GET_CONSUMER_STATUS_FROM_CLIENT = 221,
    INVOKE_BROKER_TO_RESET_OFFSET = 222,
    INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223,
    QUERY_TOPIC_CONSUME_BY_WHO = 300,
    GET_TOPICS_BY_CLUSTER = 224,
    REGISTER_FILTER_SERVER = 301,
    REGISTER_MESSAGE_FILTER_CLASS = 302,
    QUERY_CONSUME_TIME_SPAN = 303,
    GET_SYSTEM_TOPIC_LIST_FROM_NS = 304,
    GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305,
    CLEAN_EXPIRED_CONSUMEQUEUE = 306,
    GET_CONSUMER_RUNNING_INFO = 307,
    QUERY_CORRECTION_OFFSET = 308,
    CONSUME_MESSAGE_DIRECTLY = 309,
    SEND_MESSAGE_V2 = 310,
    GET_UNIT_TOPIC_LIST = 311,
    GET_HAS_UNIT_SUB_TOPIC_LIST = 312,
    GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313,
    CLONE_GROUP_OFFSET = 314,
    VIEW_BROKER_STATS_DATA = 315,
    CLEAN_UNUSED_TOPIC = 316,
    GET_BROKER_CONSUME_STATS = 317,
    UPDATE_NAMESRV_CONFIG = 318,
    GET_NAMESRV_CONFIG = 319,
    SEND_BATCH_MESSAGE = 320,
    QUERY_CONSUME_QUEUE = 321,
    QUERY_DATA_VERSION = 322,
    RESUME_CHECK_HALF_MESSAGE = 323,
    SEND_REPLY_MESSAGE = 324,
    SEND_REPLY_MESSAGE_V2 = 325,
    PUSH_REPLY_MESSAGE_TO_CLIENT = 326,
}
_M.REQUEST_CODE = REQUEST_CODE

local RESPONSE_CODE = {
    SUCCESS = 0,
    SYSTEM_ERROR = 1,
    SYSTEM_BUSY = 2,
    REQUEST_CODE_NOT_SUPPORTED = 3,
    TRANSACTION_FAILED = 4,
    FLUSH_DISK_TIMEOUT = 10,
    SLAVE_NOT_AVAILABLE = 11,
    FLUSH_SLAVE_TIMEOUT = 12,
    MESSAGE_ILLEGAL = 13,
    SERVICE_NOT_AVAILABLE = 14,
    VERSION_NOT_SUPPORTED = 15,
    NO_PERMISSION = 16,
    TOPIC_NOT_EXIST = 17,
    TOPIC_EXIST_ALREADY = 18,
    PULL_NOT_FOUND = 19,
    PULL_RETRY_IMMEDIATELY = 20,
    PULL_OFFSET_MOVED = 21,
    QUERY_NOT_FOUND = 22,
    SUBSCRIPTION_PARSE_FAILED = 23,
    SUBSCRIPTION_NOT_EXIST = 24,
    SUBSCRIPTION_NOT_LATEST = 25,
    SUBSCRIPTION_GROUP_NOT_EXIST = 26,
    FILTER_DATA_NOT_EXIST = 27,
    FILTER_DATA_NOT_LATEST = 28,
    TRANSACTION_SHOULD_COMMIT = 200,
    TRANSACTION_SHOULD_ROLLBACK = 201,
    TRANSACTION_STATE_UNKNOW = 202,
    TRANSACTION_STATE_GROUP_WRONG = 203,
    NO_BUYER_ID = 204,
    NOT_IN_CURRENT_UNIT = 205,
    CONSUMER_NOT_ONLINE = 206,
    CONSUME_MSG_TIMEOUT = 207,
    NO_MESSAGE = 208,
    UPDATE_AND_CREATE_ACL_CONFIG_FAILED = 209,
    DELETE_ACL_CONFIG_FAILED = 210,
    UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED = 211,
}
_M.RESPONSE_CODE = RESPONSE_CODE

local REQUEST_CODE_NAME, RESPONSE_CODE_NAME = {}, {}
for name, code in pairs(REQUEST_CODE) do
    REQUEST_CODE_NAME[code] = name
end
for name, code in pairs(RESPONSE_CODE) do
    RESPONSE_CODE_NAME[code] = name
end
_M.REQUEST_CODE_NAME = REQUEST_CODE_NAME
_M.RESPONSE_CODE_NAME = RESPONSE_CODE_NAME

_M.COMPRESSED_FLAG = 1;
_M.MULTI_TAGS_FLAG = 2;
_M.TRANSACTION_NOT_TYPE = 0
_M.TRANSACTION_PREPARED_TYPE = 4
_M.TRANSACTION_COMMIT_TYPE = 8
_M.TRANSACTION_ROLLBACK_TYPE = 12

_M.TRANSACTION_TYPE_MAP = {
    [_M.TRANSACTION_NOT_TYPE] = "UNKNOW",
    [_M.TRANSACTION_PREPARED_TYPE] = "UNKNOW",
    [_M.TRANSACTION_COMMIT_TYPE] = "COMMIT_MESSAGE",
    [_M.TRANSACTION_ROLLBACK_TYPE] = "ROLLBACK_MESSAGE",
}

_M.BORNHOST_V6_FLAG = lshift(1, 4)
_M.STOREHOSTADDRESS_V6_FLAG = lshift(1, 5)

_M.PERM_PRIORITY = 8
_M.PERM_READ = 4
_M.PERM_WRITE = 2
_M.PERM_INHERIT = 1

_M.RPC_TYPE = 1  -- 0 request 1 response
_M.RPC_ONEWAY = 2 -- 0 twoway, 2 oneway

_M.Normal_Msg = 0
_M.Trans_Msg_Half = 1
_M.Trans_msg_Commit = 2
_M.Delay_Msg = 3
_M.msgType = {
    [0] = "Normal_Msg",
    [1] = "Trans_Msg_Half",
    [2] = "Trans_msg_Commit",
    [3] = "Delay_Msg",
}

_M.maxMessageSize = 1024 * 1024 * 4

function _M.checkTopic(t)
    return #t < 127
end

function _M.isSystemTopic(t)
    return SYSTEM_TOPIC_SET[t] or string.find(t, SYSTEM_TOPIC_PREFIX, nil, true) == 1
end

function _M.checkMessage(m)
    return #m < 4194304
end

local function int2bin(n)
    return char(band(rshift(n, 24), 0xff)) ..
            char(band(rshift(n, 16), 0xff)) ..
            char(band(rshift(n, 8), 0xff)) ..
            char(band(n, 0xff))
end

local function short2bin(n)
    return char(band(rshift(n, 8), 0xff)) ..
            char(band(n, 0xff))
end

local requestId = 0
local function encode(code, h, body, oneway)
    local res = {
        "", -- length: fill later
        char(_M.serializeTypeCurrentRPC == "JSON" and serializeTypeJson or serializeTypeRocketmq),
        "", -- header_length: fill later
    }

    requestId = requestId + 1
    local flag = 0
    if oneway then
        flag = bor(flag, _M.RPC_ONEWAY)
    end
    local header_length
    if _M.serializeTypeCurrentRPC == "JSON" then
        local header = {
            code = code,
            language = 'other',
            flag = flag,
            opaque = requestId,
            serializeTypeCurrentRPC = _M.serializeTypeCurrentRPC,
            version = 373, -- version:4.8.0
            extFields = h,
        }
        header = cjson_safe.encode(header)
        insert(res, header)
        header_length = #header
    else
        insert(res, short2bin(code))
        insert(res, char(0x07)) -- language: other
        insert(res, short2bin(399)) -- version:4.9.3
        insert(res, int2bin(requestId))
        insert(res, int2bin(flag))
        insert(res, int2bin(0)) -- remark len:0
        insert(res, "") -- extFields len: fill later
        local ext_fields_len_pos = #res
        local ext_fields_len = 0
        for k, v in pairs(h) do
            insert(res, short2bin(#k))
            insert(res, k)
            local value = tostring(v)
            insert(res, int2bin(#value))
            insert(res, value)
            ext_fields_len = ext_fields_len + 2 + 4 + #k + #value
        end
        res[ext_fields_len_pos] = int2bin(ext_fields_len)
        header_length = 2 + 1 + 2 + 4 + 4 + 4 + 4 + ext_fields_len
    end

    body = body or ''
    local length = 4 + header_length + #body
    res[1] = int2bin(length)
    res[3] = char(band(rshift(header_length, 16), 0xff))
            .. char(band(rshift(header_length, 8), 0xff))
            .. char(band(header_length, 0xff))
    insert(res, body)
    return concat(res), requestId
end
_M.encode = encode

local function getByte(buffer, offset)
    return byte(buffer, offset), offset + 1
end

local function getShort(buffer, offset)
    local res = lshift(byte(buffer, offset), 8) +
            byte(buffer, offset + 1)
    return res, offset + 2
end

local function getInt(buffer, offset)
    local res = lshift(byte(buffer, offset), 24) +
            lshift(byte(buffer, offset + 1), 16) +
            lshift(byte(buffer, offset + 2), 8) +
            byte(buffer, offset + 3)
    return res, offset + 4
end

local function getLong(buffer, offset)
    local res1, res2
    res1, offset = getInt(buffer, offset)
    res2, offset = getInt(buffer, offset)
    local long = lshift(0ULL + res1, 32) + res2
    return tostring(long):sub(0, -4), offset
end

local function decodeHeader(recv)
    local serializeType = byte(recv, 1)
    local header_length = lshift(byte(recv, 2), 16) +
            lshift(byte(recv, 3), 8) +
            byte(recv, 4)
    if serializeType == serializeTypeJson then
        local header = string.sub(recv, 5, header_length + 4)
        return cjson_safe.decode(header), header_length
    else
        local header = {
            code = getShort(recv, 5),
            version = getShort(recv, 8),
            opaque = getInt(recv, 10),
            flag = getInt(recv, 14),
            extFields = {},
        }
        local remark_len = getInt(recv, 18)
        if remark_len > 0 then
            header.remark = string.sub(recv, 22, remark_len + 21)
        end
        local ext_fields_len, offset = getInt(recv, remark_len + 22)
        local ext_fields_end = offset + ext_fields_len
        while offset < ext_fields_end do
            local len
            len, offset = getShort(recv, offset)
            local k = string.sub(recv, offset, offset + len - 1)
            offset = offset + len

            len, offset = getInt(recv, offset)
            local v = string.sub(recv, offset, offset + len - 1)
            offset = offset + len
            header.extFields[k] = v
        end
        return header, header_length
    end
end
_M.decodeHeader = decodeHeader

local function newSocket(addr, useTLS, timeout, opt)
    local ip, port = unpack(split(addr, ':'))
    local sock = ngx_socket_tcp()
    sock:settimeout(timeout)
    sock:setkeepalive(10000, 100)

    local res, err = sock:connect(ip, port, opt)
    if not res then
        return nil, ('connect %s:%s fail:%s'):format(ip, port, err)
    end
    if useTLS then
        local ok, err = sock:sslhandshake(nil, nil, false)
        if not ok then
            return nil, "failed to do ssl handshake: " .. err
        end
    end
    return sock
end
_M.newSocket = newSocket

local function doReqeust(sock, send, requestId, oneway)
    local ok, err = sock:send(send)
    if not ok then
        return nil, nil, err
    end
    if oneway then
        return true
    end
    local header, header_length, body
    while true do
        local recv, err = sock:receive(4)
        if not recv then
            return nil, nil, err
        end
        local length = getInt(recv, 1)
        local recv, err = sock:receive(length)
        if not recv then
            return nil, nil, err
        end
        header, header_length = decodeHeader(recv)
        body = string.sub(recv, header_length + 5)
        print(('\27[34mrecv:%s\27[0m %s %s'):format((band(header.flag, _M.RPC_TYPE) > 0 and RESPONSE_CODE_NAME or REQUEST_CODE_NAME)[header.code] or header.code, header.remark or '', body))
        if header.opaque == requestId and band(header.flag, _M.RPC_TYPE) > 0 then
            break
        end
    end
    return header, body
end
_M.doReqeust = doReqeust

local function request(code, addr, header, body, oneway, RPCHook, useTLS, timeout)
    if RPCHook then
        for _, hook in ipairs(RPCHook) do
            hook:doBeforeRequest(addr, header, body)
        end
    end
    --print(('\27[33msend %s: %s %s\27[0m %s %s'):format(oneway and 'oneway' or '', addr, REQUEST_CODE_NAME[code] or code, cjson_safe.encode(header), body))
    local send, requestId = encode(code, header, body, oneway)
    local sock, err = newSocket(addr, useTLS, timeout)
    if err then
        return nil, nil, err
    end
    local respHeader, respBody, err = doReqeust(sock, send, requestId, oneway)
    if err then
        return nil, nil, err
    end
    if not oneway and RPCHook then
        for _, hook in ipairs(RPCHook) do
            hook:doAfterResponse(addr, header, body, respHeader, respBody)
        end
    end
    return respHeader, respBody, err
end
_M.request = request

_M.request1 = function(code, sock, header, body, oneway, RPCHook)
    local addr = sock[3]
    if RPCHook then
        for _, hook in ipairs(RPCHook) do
            hook:doBeforeRequest(addr, header, body)
        end
    end
    --print(('\27[33msend %s: %s %s\27[0m %s %s'):format(oneway and 'oneway' or '', addr, REQUEST_CODE_NAME[code] or code, cjson_safe.encode(header), body))
    local send, requestId = encode(code, header, body, oneway)
    local respHeader, respBody, err = doReqeust(sock, send, requestId, oneway)
    if err then
        return nil, nil, err
    end
    if not oneway and RPCHook then
        for _, hook in ipairs(RPCHook) do
            hook:doAfterResponse(addr, header, body, respHeader, respBody)
        end
    end
    return respHeader, respBody, err
end

local function decodeMsg(buffer, offset, readBody, isClient)
    local msgExt = {}
    local _
    msgExt.storeSize, offset = getInt(buffer, offset)
    _, offset = getInt(buffer, offset) --MAGICCODE
    msgExt.bodyCRC, offset = getInt(buffer, offset)
    msgExt.queueId, offset = getInt(buffer, offset)
    msgExt.flag, offset = getInt(buffer, offset)
    msgExt.queueOffset, offset = getLong(buffer, offset)
    msgExt.commitLogOffset, offset = getLong(buffer, offset)
    msgExt.sysFlag, offset = getInt(buffer, offset)
    msgExt.bornTimeStamp, offset = getLong(buffer, offset)

    local bornHostIPLength = band(msgExt.sysFlag, _M.BORNHOST_V6_FLAG) == 0 and 4 or 16;
    local bornHostIP, bornHostPort = string.sub(buffer, offset, offset + bornHostIPLength - 1)
    offset = offset + bornHostIPLength
    bornHostPort, offset = getInt(buffer, offset)
    msgExt.bornHost = utils.toIp(bornHostIP) .. ':' .. bornHostPort

    msgExt.storeTimestamp, offset = getLong(buffer, offset)

    local storeHostIPLength = band(msgExt.sysFlag, _M.STOREHOSTADDRESS_V6_FLAG) == 0 and 4 or 16;
    local storeHostIp, storeHostPort = string.sub(buffer, offset, offset + storeHostIPLength - 1)
    offset = offset + bornHostIPLength
    storeHostPort, offset = getInt(buffer, offset)
    msgExt.storeHost = utils.toIp(storeHostIp) .. ':' .. storeHostPort

    msgExt.reconsumeTimes, offset = getInt(buffer, offset)
    msgExt.preparedTransactionOffset, offset = getLong(buffer, offset)

    local bodyLen, topicLen, propertiesLength
    bodyLen, offset = getInt(buffer, offset)
    if bodyLen > 0 and readBody then
        msgExt.body = string.sub(buffer, offset, offset + bodyLen - 1)
    end
    offset = offset + bodyLen
    topicLen, offset = getByte(buffer, offset)
    msgExt.topic = string.sub(buffer, offset, offset + topicLen - 1)
    offset = offset + topicLen

    propertiesLength, offset = getShort(buffer, offset)
    msgExt.properties = utils.string2messageProperties(string.sub(buffer, offset, offset + propertiesLength - 1))
    offset = offset + propertiesLength
    msgExt.msgId = utils.createMessageId(storeHostIp, storeHostPort, msgExt.commitLogOffset)

    if isClient then
        msgExt.offsetMsgId = msgExt.msgId
    end
    return msgExt, offset
end

_M.decodeMsg = function(buffer, readBody, isClient)
    return decodeMsg(buffer, 1, readBody, isClient)
end

_M.decodeMsgs = function(msgs, buffer, readBody, isClient)
    local offset = 1
    while offset < #buffer do
        local msg
        msg, offset = decodeMsg(buffer, offset, readBody, isClient)
        insert(msgs, msg)
    end
end

local function messageProperties2String(properties)
    local res = {}
    for k, v in pairs(properties) do
        insert(res, k .. char(1) .. v .. char(2))
    end
    return concat(res, '')
end
_M.messageProperties2String = messageProperties2String

local function encodeMsg(msg)
    local properties = messageProperties2String(msg.properties);
    local storeSize = 4 -- 1 TOTALSIZE
            + 4 -- 2 MAGICCOD
            + 4 -- 3 BODYCRC
            + 4 -- 4 FLAG
            + 4 + #msg.body -- 5 BODY
            + 2 + #properties;
    return concat({
        int2bin(storeSize), -- 1 TOTALSIZE
        int2bin(0), -- 2 MAGICCOD
        int2bin(0), -- 3 BODYCRC
        int2bin(msg.flag), -- 4 FLAG
        int2bin(#msg.body), msg.body, -- 5 BODY
        short2bin(#properties), properties, -- 6 properties
    })
end
_M.encodeMsg = encodeMsg

return _M
