# vim:set ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty/lualib/?.so;/usr/local/openresty/lualib/?.so;;";

    init_by_lua_block{
        require 'resty.core'
    }
};

no_long_string();
#no_diff();

no_shuffle();
run_tests();

__DATA__

=== TEST 1: produce
--- http_config eval: $::HttpConfig
--- config
    location /benchmark {
        content_by_lua_block {
            local admin = require "resty.rocketmq.admin"
            local producer = require "resty.rocketmq.producer"

            local adm, err = admin.new({ "127.0.0.1:9876"})
            adm:createTopic("TopicTest", 8)

            local args = ngx.req.get_uri_args()
            local thread_num = tonumber(args.thread_num) or 4
            local msg_size = tonumber(args.msg_size) or 120
            local topic = args.topic or "TopicTest"
            local trace_enable = args.trace_enable == 'true'
            local delay_level = args.delay_level
            local use_tls = args.use_tls == 'true'
            local batch_size = tonumber(args.batch_size) or 1
            if batch_size > 1 and delay_level then
                ngx.say("delay_level not supported in batch")
                return
            end

            local total_success = 0
            local total_error = 0
            local running = true
            for i = 1, thread_num do
                ngx.thread.spawn(function()
                    local message_buffer = {}
                    for i = 1, msg_size/10 do
                        table.insert(message_buffer, '0123456789')
                    end
                    local message = table.concat(message_buffer)
                    local p = producer.new({ "127.0.0.1:9876"}, "produce_group")
                    p:setUseTLS(use_tls)
                    while running do
                        local res, err
                        if batch_size == 1 then
                            res, err = p:send(topic, message,"tag","key", {DELAY=delay_level})
                        else
                            local msgs = {}
                            for i = 1, batch_size do
                                table.insert(msgs, {topic=topic,body=message,tags="tag",keys="key"})
                            end
                            res, err = p:batchSend(msgs)
                        end
                        if res then
                            total_success = total_success + batch_size
                        else
                            total_error = total_error + 1
                        end
                    end
                end)
            end

            local last_time = ngx.now()
            local last_success = total_success
            for i = 1, 10 do
                ngx.sleep(1)
                ngx.update_time()
                local time = ngx.now()
                local success = total_success
                ngx.say("tps=", (success - last_success) / (time - last_time),',total_error=', total_error)
                ngx.flush()
                last_time = time
                last_success = success
            end
            running = false
        }
    }
--- request
GET /benchmark?thread_num=8
--- response_body_like
tps=.*,total_error=0
tps=.*,total_error=0
tps=.*,total_error=0
tps=.*,total_error=0
tps=.*,total_error=0
tps=.*,total_error=0
tps=.*,total_error=0
tps=.*,total_error=0
tps=.*,total_error=0
tps=.*,total_error=0
--- no_error_log
[error]


