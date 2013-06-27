# This monkey-patch must be removed when ticket #44 has been included in
# an updated ZooKeeper Gem
#   Ticket:       https://github.com/slyphon/zookeeper/issues/44
#   Pull Request: https://github.com/slyphon/zookeeper/pull/45

module Zookeeper
  class JavaBase

    def get(req_id, path, callback, watcher)
      handle_keeper_exception do
        watch_cb = watcher ? create_watcher(req_id, path) : false

        if callback
          jzk.getData(path, watch_cb, JavaCB::DataCallback.new(req_id), event_queue)
          [Code::Ok, nil, nil]    # the 'nil, nil' isn't strictly necessary here
        else # sync
          stat = JZKD::Stat.new
          value = jzk.getData(path, watch_cb, stat)
          data = String.from_java_bytes(value) unless value.nil?

          [Code::Ok, data, stat.to_hash]
        end
      end
    end

  end
end