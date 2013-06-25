require 'thread_safe'
require 'semantic_logger'
require 'zk'

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
    #
    # Warning: Due to the way ZooKeeper works, if any node is set to a value of ''
    #          then no on_update notifications will be sent.
    #          On the other hand on_delete notifications will be invoked
    #          for every element in a path including the leaf node.
    #          This is because in ZooKeeper directories can also have a value,
    #          and all nodes created will have at least a value of ''
    class Registry
      # Logging instance for this class
      include SemanticLogger::Loggable

      attr_reader :root, :zookeeper

      # Create a Registry instance to manage a information within Zookeeper
      #
      # :root [String]
      #   Root key to load and then monitor for changes
      #   It is not recommended to set the root to "/" as it will generate
      #   significant traffic since it will also monitor ZooKeeper Admin changes
      #   Mandatory
      #
      # :zookeeper [Hash|ZK]
      #   ZooKeeper configuration information, or an existing
      #   ZK ( ZooKeeper client) instance
      #
      #   :servers [Array of String]
      #     Array of URL's of ZooKeeper servers to connect to with port numbers
      #     ['server1:2181', 'server2:2181']
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

        zookeeper_config = params.delete(:zookeeper) || {}
        if zookeeper_config.is_a?(ZK::Client::Base)
          @zookeeper = zookeeper_config
        else
          servers = zookeeper_config.delete(:servers) || ['127.0.0.1:2181']

          # Generate warning log entries for any unknown configuration options
          zookeeper_config.each_pair {|k,v| logger.warn "Ignoring unknown configuration option: zookeeper.#{k}"}

          # Create Zookeeper connection
          #   server1:2181,server2:2181,server3:2181
          @zookeeper = ZK.new(servers.join(','))
        end

        # Allow the serializer and deserializer implementations to be replaced
        @serializer   = params.delete(:serializer)   || RubySkynet::Zookeeper::Json::Serializer
        @deserializer = params.delete(:deserializer) || RubySkynet::Zookeeper::Json::Deserializer

        # Generate warning log entries for any unknown configuration options
        params.each_pair {|k,v| logger.warn "Ignoring unknown configuration option: #{k}"}

        # Hash with Array values containing the list of children fro each node, if any
        @subscriptions = ThreadSafe::Hash.new

        # Create the root path if it does not already exist
        @zookeeper.mkdir_p(@root) unless (@root == '/' || @zookeeper.exists?(@root))

        # Start watching registry for any changes
        get_recursive(@root, watch=true, &block)

        at_exit do
          close
        end
      end

      # Retrieve the latest value from a specific path from the registry
      # Returns nil when the key is not present in the registry
      def [](key)
        begin
          value, stat = @zookeeper.get(full_key(key))
          @deserializer.deserialize(value)
        rescue ZK::Exceptions::NoNode
          nil
        end
      end

      # Replace the latest value at a specific key
      def []=(key,value)
        v = @serializer.serialize(value)
        k = full_key(key)
        begin
          @zookeeper.set(k, v)
        rescue ZK::Exceptions::NoNode
          create_path(k, v)
        end
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
        begin
          paths = key.split('/')
          paths.pop
          @zookeeper.delete(full_key(key))
          if remove_empty_parents
            while paths.size > 0
              parent_path = full_key(paths.join('/'))
              value, stat = @zookeeper.get(parent_path)
              break if value != '' || (stat.num_children > 0)
              @zookeeper.delete(parent_path)
              paths.pop
            end
          end
        rescue ZK::Exceptions::NoNode
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
      #      New value from doozer
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
      #      The key that was updated in doozer
      #      Supplying a key of '*' means all paths
      #      Default: '*'
      #
      #    value
      #      New value from doozer
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
      #      The key that was deleted from doozer
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

      Subscription = Struct.new(:subscription, :children)

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
      def create_path(full_path, value='')
        paths = full_path.split('/')
        # Don't create the child node yet
        paths.pop
        paths.shift
        path = ''
        paths.each do |p|
          path << "/#{p}"
          @zookeeper.create(path) unless @zookeeper.exists?(path)
        end
        @zookeeper.create(full_path, value)
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
      def get_recursive(full_path, watch=false, &block)
        # Subscription block to call for watch events
        @watch_proc ||= Proc.new do |event|
          if event.node_changed?
            path = event.path
            logger.debug "Node '#{path}' Changed", event.inspect

            # Fetch current value and re-subscribe
            value, stat = @zookeeper.get(path, :watch => true)

            # Don't call change for intermediate nodes in the tree unless they have a value
            node_updated(relative_key(path), @deserializer.deserialize(value), stat.version) #if (value != '') || (stat.num_children == 0)

          elsif event.node_deleted?
            # A node has been deleted
            # TODO How to ignore child deleted when it is a directory, not a leaf
            path = event.path
            logger.debug "Node '#{path}' Deleted", event.inspect
            if subscription = @subscriptions.delete(path)
              subscription.subscription.unregister
            end
            node_deleted(relative_key(path))

          elsif event.node_child?
            # The list of nodes has changed - Does not say if it was added or removed
            path = event.path
            logger.debug "Node '#{path}' Child changed", event.inspect
            begin
              current_children = @zookeeper.children(path, :watch => true)
              previous_children = nil

              # Only register if not already registered.
              subscription = @subscriptions[path]
              if subscription
                previous_children = subscription.children
                subscription.children = current_children
              else
                @subscriptions[path] = Subscription.new(@zookeeper.register(path, &@watch_proc), current_children)
              end

              # Created Child Nodes
              new_nodes = previous_children ? (current_children - previous_children) : current_children
              new_nodes.each do |child|
                get_recursive(File.join(path,child), true) do |key, value, version|
                  node_created(key, value, version)
                end
              end
            rescue ZK::Exceptions::NoNode
              # This node itself may have already been removed
              # node_deleted? above will remove its subscription
            end
            # Ignore Deleted Child Nodes since they will also get event.node_deleted?
          elsif event.node_created?
            # Node created events are only created for paths that were deleted
            # and then created again
            # No op - This is covered by node_child created event
            logger.debug "Node '#{event.path}' Created - No op", event.inspect
          else
            # TODO Need to re-load registry when re-connected
            logger.warn "Ignoring unknown event", event.inspect
          end
        end

        node_count = 0
        begin
          # Register the Watch Block against this path
          subscription = @zookeeper.register(full_path, &@watch_proc) if watch

          # Get value for this node
          value, stat = @zookeeper.get(full_path, :watch => watch)

          # ZooKeeper assigns an empty string to all parent nodes when no value is supplied
          # Call block if this is a leaf node, or if it is a parent node with a value
          if block && value != '' #((stat.num_children == 0) || value != '')
            block.call(relative_key(full_path), @deserializer.deserialize(value), stat.version)
          end

          # Ephemeral nodes cannot have children
          if !stat.ephemeral? && (watch || (stat.num_children > 0))
            # Also watch this node for child changes
            children = @zookeeper.children(full_path, :watch => watch)

            # Save the current list of children so that we can figure out what
            # a child changed event actually means
            @subscriptions[full_path] = Subscription.new(subscription, children) if watch

            # Also watch children nodes
            children.each do |child|
              node_count += get_recursive(File.join(full_path,child), watch, &block)
            end
          else
            @subscriptions[full_path] = Subscription.new(subscription, nil) if watch
          end

          node_count += 1
        rescue ZK::Exceptions::NoNode
        end
        node_count
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

    end
  end
end