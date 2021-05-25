local cjson_safe = require("cjson.safe")
local split = require("resty.rocketmq.utils").split
local bit = require("bit")
local rshift = bit.rshift
local band = bit.band
local bor = bit.bor
local lshift = bit.lshift
local concat = table.concat
local char = string.char
local ngx_socket_tcp = ngx.socket.tcp
local unpack = unpack
local byte = string.byte

local _M = {}

_M.SYSTEM_TOPIC_PREFIX = 'rmq_sys_'
_M.CID_RMQ_SYS_PREFIX = 'CID_RMQ_SYS_'
_M.RETRY_GROUP_TOPIC_PREFIX = '%RETRY%'
_M.DLQ_GROUP_TOPIC_PREFIX = '%DLQ%'

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

local function codeName(c)
    for name, code in pairs(REQUEST_CODE) do
        if code == c then
            return name
        end
    end
end
_M.codeName = codeName

local function respCodeName(c)
    for name, code in pairs(RESPONSE_CODE) do
        if code == c then
            return name
        end
    end
end
_M.respCodeName = respCodeName

_M.TRANSACTION_NOT_TYPE = 0
_M.TRANSACTION_PREPARED_TYPE = 4
_M.TRANSACTION_COMMIT_TYPE = 8
_M.TRANSACTION_ROLLBACK_TYPE = 12

_M.PERM_PRIORITY = 8
_M.PERM_READ = 4
_M.PERM_WRITE = 2
_M.PERM_INHERIT = 1

_M.RPC_TYPE = 1  -- 0 request 1 response
_M.RPC_ONEWAY = 2 -- 0 twoway, 2 oneway

local VALID_PATTERN_STR = "^[%|a-zA-Z0-9_-]+$"

function _M.checkTopic(t)
    return ngx.re.match(t, VALID_PATTERN_STR, 'jo') and #t < 127
end

function _M.checkMessage(m)
    return #m < 4194304
end

local requestId = 0
local function encode(code, h, body, oneway)
    requestId = requestId + 1
    local header = {
        code = code,
        language = 'other',
        flag = 0,
        opaque = requestId,
        serializeTypeCurrentRPC = 'JSON',
        version = 373,
        extFields = h,
    }
    if oneway then
        header.flag = bor(header.flag, _M.RPC_ONEWAY)
    end
    header = cjson_safe.encode(header)
    body = body or ''
    local length = 4 + #header + #body
    local header_length = #header
    local res = {
        char(band(rshift(length, 24), 0xff)),
        char(band(rshift(length, 16), 0xff)),
        char(band(rshift(length, 8), 0xff)),
        char(band(length, 0xff)),

        char(0x00),
        char(band(rshift(header_length, 16), 0xff)),
        char(band(rshift(header_length, 8), 0xff)),
        char(band(header_length, 0xff)),
    }
    return concat(res, '') .. header .. body
end
_M.encode = encode

local function doReqeust(ip, port, send, oneway)
    local sock = ngx_socket_tcp()
    local res, err = sock:connect(ip, port)
    if not res then
        return nil, nil, ('connect %s:%s fail:%s'):format(ip, port, err)
    end
    local ok, err = sock:send(send)
    if not ok then
        return nil, nil, err
    end
    if oneway then
        return true
    end
    local recv, err = sock:receive(4)
    if not recv then
        return nil, nil, err
    end
    local length = lshift(byte(recv, 1), 24) +
            lshift(byte(recv, 2), 16) +
            lshift(byte(recv, 3), 8) +
            byte(recv, 4)
    local recv, err = sock:receive(length)
    if not recv then
        return nil, nil, err
    end
    sock:setkeepalive(10000, 100)
    local header_length = lshift(byte(recv, 2), 16) +
            lshift(byte(recv, 3), 8) +
            byte(recv, 4)
    local header = string.sub(recv, 5, header_length + 4)
    local body = string.sub(recv, header_length + 5)
    return cjson_safe.decode(header), body
end
_M.doReqeust = doReqeust

local function request(code, addr, header, body, oneway, RPCHook)
    if RPCHook then
        for _, hook in ipairs(RPCHook) do
            hook.doBeforeRequest(addr, header, body)
        end
    end
    ngx.log(ngx.DEBUG, ('\27[33msend:%s\27[0m %s %s'):format(codeName(code), cjson_safe.encode(header), body))
    local send = encode(code, header, body, oneway)
    local ip, port = unpack(split(addr, ':'))
    local respHeader, respBody, err = doReqeust(ip, port, send, oneway)
    ngx.log(ngx.DEBUG, ('\27[34mrecv:%s\27[0m %s %s'):format(respCodeName(respHeader.code), respHeader.remark or '', respBody))
    if not oneway and RPCHook then
        for _, hook in ipairs(RPCHook) do
            hook.doAfterResponse(addr, header, body, respHeader, respBody)
        end
    end
    return respHeader, respBody, err
end
_M.request = request

return _M
