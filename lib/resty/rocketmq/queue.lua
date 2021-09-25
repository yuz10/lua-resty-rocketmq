local _M = {}
function _M.new ()
    return { first = 0, last = -1 }
end

function _M.push (queue, value)
    local last = queue.last + 1
    queue.last = last
    queue[last] = value
end

function _M.pop (queue)
    local first = queue.first
    if first > queue.last then
        return nil, "list is empty"
    end
    local value = queue[first]
    queue[first] = nil        -- to allow garbage collection
    queue.first = first + 1
    return value
end

return _M