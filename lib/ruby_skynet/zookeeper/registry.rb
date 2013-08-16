require 'thread_safe'
require 'semantic_logger'
require 'zookeeper'

# Replace the Zookeeper logger for consistency
::Zookeeper.logger = SemanticLogger[::Zookeeper]
# Map Zookeeper debug logging to trace level to reduce verbosity in logs
::Zookeeper.logger.instance_eval "def debug(*args,&block)\n  trace(*args,&block)\nend"

module RubySkynet
  module Zookeeper
    #
    # Registry
    #
    # Store information in Zookeepr and subscribe to future changes
    #
    # Notifies registered subscribers when information has changed
    #
    # All paths specified are relative to the root. As such the root key
    # is never returned, nor is it required when a key is supplied as input.
    # For example, with a root of /foo/bar, any paths passed in will leave
    # out the root: host/name
    class Registry
      # Logging instance for this class
      include SemanticLogger::Loggable

      attr_reader :root

      # Create a Registry instance to manage a information within Zookeeper
      #
      # :root [String]
      #   Root key to load and then monitor for changes
      #   It is not recommended to set the root to "/" as it will generate
      #   significant traffic since it will also monitor ZooKeeper Admin changes
      #   Mandatory
      #
      # :ephemeral [Boolean]
      #   All set operations of non-nil values will result in ephemeral nodes.
      #
      # :registry [Hash|ZooKeeper]
      #   ZooKeeper configuration information, or an existing
      #   ZooKeeper ( ZooKeeper client) instance
      #
      #   :servers [Array of String]
      #     Array of URL's of ZooKeeper servers to connect to with port numbers
      #     ['server1:2181', 'server2:2181']
      #
      #   :connect_timeout [Float]
      #     Time in seconds to timeout when trying to connect to the server
      #
      # Optional Block
      #   The block will be called for every key found in the registry on startup
      #
      # Example:
      #
      #   require 'ruby_skynet/zookeeper'
      #   registry = RubySkynet::Zookeeper::Registry.new(root: '/registry') do |key, value, version|
      #     puts "Found #{key} => '#{value}' V#{version}"
      #   end
      #
      def initialize(params, &block)
        params = params.dup
        @root = params.delete(:root)
        raise "Missing mandatory parameter :root" unless @root

        # Add leading '/' to root if missing
        @root = "/#{@root}" unless @root.start_with?('/')

        # Strip trailing '/' if supplied
        @root = @root[0..-2] if @root.end_with?("/")
        @root_with_trail = "#{@root}/"
        @root = '/' if @root == ''

        registry_config = params.delete(:registry) || {}

        #   server1:2181,server2:2181,server3:2181
        @servers = (registry_config.delete(:servers) || ['127.0.0.1:2181']).join(',')
        @connect_timeout = (registry_config.delete(:connect_timeout) || 10).to_f

        # Generate warning log entries for any unknown configuration options
        registry_config.each_pair {|k,v| logger.warn "Ignoring unknown configuration option: zookeeper.#{k}"}

        # Allow the serializer and deserializer implementations to be replaced
        @serializer   = params.delete(:serializer)   || RubySkynet::Zookeeper::Json::Serializer
        @deserializer = params.delete(:deserializer) || RubySkynet::Zookeeper::Json::Deserializer

        @ephemeral = params.delete(:ephemeral)
        @ephemeral = false if @ephemeral.nil?

        # Generate warning log entries for any unknown configuration options
        params.each_pair {|k,v| logger.warn "Ignoring unknown configuration option: #{k}"}

        # Hash with Array values containing the list of children for each node, if any
        @children = ThreadSafe::Hash.new

        # Block is used in init
        @block = block

        self.init
      end

      # Retrieve the latest value from a specific path from the registry
      # Returns nil when the key is not present in the registry
      def [](key)
        result = @zookeeper.get(:path => full_key(key))
        case result[:rc]
        when ::Zookeeper::ZOK
          @deserializer.deserialize(result[:data])
        when ::Zookeeper::ZNONODE
          # Return nil if node not present
        else
          check_rc(result)
        end
      end

      # Replace the latest value at a specific key
      # Supplying a nil value will result in the key being deleted in ZooKeeper
      def []=(key,value)
        if value.nil?
          delete(key)
          return value
        end
        v = @serializer.serialize(value)
        k = full_key(key)
        result = @zookeeper.set(:path => k, :data => v)
        if result[:rc] == ::Zookeeper::ZNONODE
          create_path(k, v)
        else
          check_rc(result)
        end
        value
      end

      # Delete the value at a specific key and any parent nodes if they
      # don't have any children or values
      #
      # Params
      #   remove_empty_parents
      #     If set to true it will also delete any parent nodes that have no
      #     children or value
      #
      # Returns nil
      def delete(key, remove_empty_parents=true)
        result = @zookeeper.delete(:path => full_key(key))
        return if result[:rc] == ::Zookeeper::ZNONODE
        check_rc(result)

        if remove_empty_parents
          paths = key.split('/')
          paths.pop
          while paths.size > 0
            parent_path = full_key(paths.join('/'))
            result = @zookeeper.get(:path => parent_path)
            break if (result[:rc] == ::Zookeeper::ZNONODE) || (result[:data] != nil)

            delete(parent_path)
            paths.pop
          end
        end
        nil
      end

      # Iterate over every key, value pair in the registry
      # Optional relative path can be supplied
      # Returns the number of nodes iterated over
      #
      # Example:
      #   registry.each_pair {|k,v| puts "#{k} => #{v}"}
      def each_pair(relative_path = '', &block)
        get_recursive(full_key(relative_path), watch=false, &block)
      end

      # Returns [Array<String>] all keys in the registry
      def keys
        keys = []
        each_pair {|k,v| keys << k}
        keys
      end

      # Returns a copy of the registry as a Hash
      def to_h
        h = {}
        each_pair {|k,v| h[k] = v}
        h
      end

      # Cleanup on process termination
      def close
        @zookeeper.close if @zookeeper
        @zookeeper = nil
      end

      # When an entry is created the block will be called
      #  Parameters
      #    key
      #      The relative key to watch for changes
      #    block
      #      The block to be called
      #
      #  Parameters passed to the block:
      #    key
      #      The key that was created
      #      Supplying a key of '*' means all paths
      #      Default: '*'
      #
      #    value
      #      New value from the registry
      #
      #    version
      #      The version number of this node
      #
      # Example:
      #   registry.on_update do |key, value, revision|
      #     puts "#{key} was created with #{value}"
      #   end
      #
      # Note: They key must either be the exact path or '*' for all keys
      def on_create(key='*', &block)
        ((@create_subscribers ||= ThreadSafe::Hash.new)[key] ||= ThreadSafe::Array.new) << block
      end

      # When an entry is updated the block will be called
      #  Parameters
      #    key
      #      The relative key to watch for changes
      #    block
      #      The block to be called
      #
      #  Parameters passed to the block:
      #    key
      #      The key that was updated in the registry
      #      Supplying a key of '*' means all paths
      #      Default: '*'
      #
      #    value
      #      New value from the registry
      #
      #    version
      #      The version number of this node
      #
      # Example:
      #   registry.on_update do |key, value, version|
      #     puts "#{key} was updated to #{value}"
      #   end
      #
      # Note: They key must either be the exact path or '*' for all keys
      def on_update(key='*', &block)
        ((@update_subscribers ||= ThreadSafe::Hash.new)[key] ||= ThreadSafe::Array.new) << block
      end

      # When an entry is deleted the block will be called
      #  Parameters
      #    key
      #      The relative key to watch for changes
      #    block
      #      The block to be called
      #
      #  Parameters passed to the block:
      #    key
      #      The key that was deleted from the registry
      #      Supplying a key of '*' means all paths
      #      Default: '*'
      #
      # Example:
      #   registry.on_delete do |key, revision|
      #     puts "#{key} was deleted"
      #   end
      #
      # Note: They key must either be the exact path or '*' for all keys
      def on_delete(key='*', &block)
        ((@delete_subscribers ||= ThreadSafe::Hash.new)[key] ||= ThreadSafe::Array.new) << block
      end

      ##########################################
      protected

      # Returns the full key given a relative key
      def full_key(relative_key)
        relative_key = strip_slash(relative_key)
        relative_key == '' ? @root : File.join(@root,relative_key)
      end

      # Returns the full key given a relative key
      def relative_key(full_key)
        key = full_key.sub(@root_with_trail, '')
        key == '' ? '/' : key
      end

      ##########################################
      protected

      # Strip leading and trailing '/'
      def strip_slash(path)
        path = path[1..-1] if path.start_with?('/')
        path = path[0..-2] if path.end_with?('/')
        path
      end

      # Create the supplied path and set the supplied value
      #   Navigates through tree and creates all required parents with no values
      #   as needed to create child node with its value
      # Note: Value must already be serialized
      def create_path(full_path, value=nil)
        paths = full_path.split('/')
        # Don't create the child node yet
        paths.pop
        paths.shift
        path = ''
        paths.each do |p|
          path << "/#{p}"
          # Ignore errors since it may already exist
          @zookeeper.create(:path => path)
        end
        if value
          @zookeeper.create(:path => full_path, :data => value, :ephemeral => @ephemeral)
        else
          @zookeeper.create(:path => full_path)
        end
      end

      # returns the watcher proc for this registry instance
      def watcher
        # Subscription block to call for watch events
        @watch_proc ||= Proc.new do |event_hash|
          begin
            path = event_hash[:path]
            logger.trace "Event Received", event_hash
            case event_hash[:type]
            when ::Zookeeper::ZOO_CHANGED_EVENT
              logger.debug "Node '#{path}' Changed", event_hash

              # Fetch current value and re-subscribe
              result = @zookeeper.get(:path => path, :watcher => @watch_proc)
              check_rc(result)
              value = @deserializer.deserialize(result[:data])
              stat = result[:stat]

              # Invoke on_update callbacks
              node_updated(relative_key(path), value, stat.version)

            when ::Zookeeper::ZOO_DELETED_EVENT
              # A node has been deleted
              # TODO How to ignore child deleted when it is a directory, not a leaf
              logger.debug "Node '#{path}' Deleted", event_hash
              @children.delete(path)
              node_deleted(relative_key(path))

            when ::Zookeeper::ZOO_CHILD_EVENT
              # The list of nodes has changed - Does not say if it was added or removed
              logger.debug "Node '#{path}' Child changed", event_hash
              result = @zookeeper.get_children(:path => path, :watcher => @watch_proc)

              # This node could have been deleted already
              if result[:rc] == ::Zookeeper::ZOK
                current_children = result[:children]
                previous_children = @children[path]

                # Save children so that we can later identify new children
                @children[path] = current_children

                # New Child Nodes
                new_nodes = previous_children ? (current_children - previous_children) : current_children
                new_nodes.each do |child|
                  get_recursive(File.join(path,child), true) do |key, value, version|
                    node_created(key, value, version)
                  end
                end
                # Ignore Deleted Child Nodes since they will be handled by the Deleted Node event
              end

            when ::Zookeeper::ZOO_CREATED_EVENT
              # Node created events are only created for paths that were deleted
              # and then created again
              # No op - This is covered by node_child created event
              logger.debug "Node '#{path}' Created - No op", event_hash

            when ::Zookeeper::ZOO_SESSION_EVENT
              logger.debug "Session Event: #{@zookeeper.state_by_value(event_hash[:state])}", event_hash

              # Replace zookeeper connection since it is stale. Only react to global request
              # since this event will be received for every node being watched.
              #   Do not close the current connection since this background watcher thread is running
              #   as part of the current zookeeper connection
              #     event_hash => {:req_id=>-1, :type=>-1, :state=>-112, :path=>"", :context=>nil}
              Thread.new { self.init } if (event_hash[:req_id] == -1) && (event_hash[:state] == ::Zookeeper::ZOO_EXPIRED_SESSION_STATE)

            when ::Zookeeper::ZOO_NOTWATCHING_EVENT
              logger.debug "Ignoring ZOO_NOTWATCHING_EVENT", event_hash

            else
              # TODO Need to re-load registry when re-connected
              logger.warn "Ignoring unknown event", event_hash
            end
          rescue ::Zookeeper::Exceptions::ZookeeperException => exc
            logger.warn "Watching thread failed due to Zookeeper failure", exc
          rescue Exception => exc
            logger.error "Watching thread failed due to unhandled exception", exc
          end
        end
      end

      # Recursively fetches all the values in the registry and optionally
      # registers and starts watching all nodes in ZooKeeper
      # Returns the number of nodes iterated over
      #
      # Optionally supply a block of code to be called when each node
      # is being subscribed to. It calls the block supplying the value of that node
      # along with its relative path
      #
      # Example:
      #  get_recursive(full_key(relative_path), true)
      def get_recursive(full_path, watch=false, create_path=true, &block)
        watch_proc = watcher if watch

        # Get value for this node
        result = @zookeeper.get(:path => full_path, :watcher => watch_proc)

        # Create the path if it does not exist
        if create_path && (result[:rc] == ::Zookeeper::ZNONODE)
          create_path(full_path)
          result = @zookeeper.get(:path => full_path, :watcher => watch_proc)
        end

        # Cannot find this node
        return 0 if result[:rc] == ::Zookeeper::ZNONODE

        check_rc(result)
        value = @deserializer.deserialize(result[:data])
        stat = result[:stat]

        # ZooKeeper assigns a nil value to all parent nodes when no value is supplied
        # Call block if this is a leaf node, or if it is a parent node with a value
        if block && ((stat.num_children == 0) || value != nil)
          block.call(relative_key(full_path), value, stat.version, stat.num_children)
        end

        # Iterate over children if any
        node_count = 1
        # Ephemeral nodes cannot have children
        if !(stat.ephemeral_owner && (stat.ephemeral_owner != 0)) && (watch || (stat.num_children > 0))
          # Also watch this node for child changes
          result = @zookeeper.get_children(:path => full_path, :watcher => watch_proc)

          # This node could have been deleted already
          if result[:rc] == ::Zookeeper::ZOK
            children = result[:children]

            # Save the current list of children so that we can figure out what
            # a child changed event actually means
            @children[full_path] = children if watch

            # Also watch children nodes
            children.each do |child|
              node_count += get_recursive(File.join(full_path,child), watch, &block)
            end
          end
        end
        node_count
      end

      # Checks the return code from ZooKeeper and raises an exception if it is non-zero
      def check_rc(result)
        if result[:rc] != ::Zookeeper::ZOK
          logger.error "Zookeeper failure", result
          ::Zookeeper::Exceptions.raise_on_error(result[:rc])
        end
      end

      # The key was created in the registry
      def node_created(key, value, version)
        logger.debug "Created: #{key}", value

        return unless @create_subscribers

        # Subscribers to specific paths
        if subscribers = @create_subscribers[key]
          subscribers.each{|subscriber| subscriber.call(key, value, version)}
        end

        # Any subscribers for all events?
        if all_subscribers = @create_subscribers['*']
          all_subscribers.each{|subscriber| subscriber.call(key, value, version)}
        end
      end

      # The key was updated in the registry
      def node_updated(key, value, version)
        logger.debug "Updated: #{key}", value

        return unless @update_subscribers

        # Subscribers to specific paths
        if subscribers = @update_subscribers[key]
          subscribers.each{|subscriber| subscriber.call(key, value, version)}
        end

        # Any subscribers for all events?
        if all_subscribers = @update_subscribers['*']
          all_subscribers.each{|subscriber| subscriber.call(key, value, version)}
        end
      end

      # An existing key was removed from the registry
      def node_deleted(key)
        logger.debug { "Deleted: #{key}" }

        return unless @delete_subscribers

        # Subscribers to specific paths
        if subscribers = @delete_subscribers[key]
          subscribers.each{|subscriber| subscriber.call(key)}
        end

        # Any subscribers for all events?
        if all_subscribers = @delete_subscribers['*']
          all_subscribers.each{|subscriber| subscriber.call(key)}
        end
      end

      # Create zookeeper conenction and start watching the registry for any changes
      def init
        logger.benchmark_info "Connected to Zookeeper" do
          @zookeeper.close if @zookeeper
          # Create Zookeeper connection
          @zookeeper = ::Zookeeper.new(@servers, @connect_timeout, watcher)
          at_exit do
            @zookeeper.close if @zookeeper
          end

          # Start watching registry for any changes
          get_recursive(@root, watch=true, create_path=true, &@block)
        end
      end

    end
  end
end