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
        @child_list = ThreadSafe::Hash.new

        # Load up registry
        watch_recursive(@root, &block)

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

      # Delete the value at a specific key
      # Returns nil
      def delete(key)
        begin
          @zookeeper.delete(full_key(key))
        rescue ZK::Exceptions::NoNode
        end
        nil
      end

      # Iterate over every key, value pair in the registry
      # Optional relative path can be supplied
      #
      # Example:
      #   registry.each_pair {|k,v| puts "#{k} => #{v}"}
      def each_pair(relative_path = '', &block)
        begin
          relative_path = relative_path[0..-2] if relative_path.end_with?("/")

          full_path = full_key(relative_path)
          value, stat = @zookeeper.get(full_path)
          if stat.num_children > 0
            # Does parent node have a value?
            #   ZooKeeper assigns an empty string to all parent nodes when no value is supplied
            block.call(relative_path, @deserializer.deserialize(value)) if value != ''

            @zookeeper.children(full_path).each do |child|
              each_pair("#{relative_path}/#{child}", &block)
            end
          else
            block.call(relative_path, @deserializer.deserialize(value))
          end
        rescue ZK::Exceptions::NoNode
        end
        nil
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
      # Example:
      #   registry.on_update do |key, value, version|
      #     puts "#{key} was updated to #{value}"
      #   end
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
      def on_delete(key='*', &block)
        ((@delete_subscribers ||= ThreadSafe::Hash.new)[key] ||= ThreadSafe::Array.new) << block
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
      # Example:
      #   registry.on_update do |key, value, revision|
      #     puts "#{key} was created with #{value}"
      #   end
      def on_create(key='*', &block)
        ((@create_subscribers ||= ThreadSafe::Hash.new)[key] ||= ThreadSafe::Array.new) << block
      end

      ##########################################
      protected

      # Returns the full key given a relative key
      def full_key(relative_key)
        relative_key = strip_slash(relative_key)
        relative_key == '' ? @root : "#{@root}/#{relative_key}"
      end

      # Returns the full key given a relative key
      def relative_key(full_key)
        full_key.sub(@root_with_trail, '')
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
      def create_path(full_path, value)
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

      # Start watching the specified path and all its children
      # Returns the number of nodes now being watched
      #
      # Optionally Supply a block of code to be called when each node
      # is being subscribed to. It calls the block supplying the value of that node
      # along with its relative path
      # Very useful when loading a copy of the registry into memory
      #
      # Example:
      #  watch_recursive(full_key(relative_path))
      def watch_recursive(full_path, &block)
        # Subscription block to call for watch events
        @watch_proc ||= Proc.new do |event|
          if event.node_changed?
            path = event.path
            logger.debug "Node '#{path}' Changed", event.inspect

            # Fetch current value and re-subscribe
            value, stat = @zookeeper.get(path, :watch => true)

            # Don't call change for intermediate nodes in the tree unless they have a value
            changed(relative_key(path), value, stat.version) #if (value != '') || (stat.num_children == 0)

          elsif event.node_deleted?
            # A node has been deleted
            # TODO How to ignore child deleted when it is a directory, not a leaf
            # TODO Remove registration for this key
            path = event.path
            logger.debug "Node Deleted", event.inspect
            deleted(relative_key(path))

          elsif event.node_child?
            # The list of nodes has changed - Does not say if it was added or removed
            path = event.path
            logger.debug "Node '#{path}' Child changed", event.inspect
            previous_children = @child_list[path]
            current_children = @zookeeper.children(path, :watch => true)
            @child_list[path] = current_children

            # Only register if not already registered.
            # Example node deleted then added back
            @zookeeper.register(path, @@watch_proc) unless previous_children

            # Created Child Nodes
            new_nodes = previous_children ? (current_children - previous_children) : current_children
            new_nodes.each do |child|
              watch_recursive("#{path}/#{child}") do |key, value, version|
                created(key, value, version)
              end
            end
            # Ignore Deleted Child Nodes since they will also get event.node_deleted?
          elsif event.node_created?
            # Node created events are only created for paths that were deleted
            # and then created again
            # No op - This is covered by node_child created event
            logger.debug "Node '#{event.path}' Created - No op", event.inspect
          else
            logger.warn "Ignoring unknown event", event.inspect
          end
        end

        node_count = 0
        begin
          # Register the Watch Block against this path
          @zookeeper.register(full_path, &@watch_proc)

          # Watch node value for changes
          value, stat = @zookeeper.get(full_path, :watch => true)
          block.call(relative_key(full_path), value, stat.version) if block && (value != '')
          node_count += 1

          # Ephemeral nodes cannot have children
          unless stat.ephemeral?
            # Also watch this node for child changes
            children = @zookeeper.children(full_path, :watch => true)

            # Save the current list of children so that we can figure out what
            # a child changed event actually means
            @child_list[full_path] = children

            # Also watch children nodes
            children.each do |child|
              node_count += watch_recursive("#{full_path}/#{child}", &block)
            end
          end
        rescue ZK::Exceptions::NoNode
        end
        node_count
      end

      # The key was created in the registry
      def created(key, value, version)
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

      # The key was added or updated in the registry
      def changed(key, value, version)
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
      def deleted(key, revision)
        logger.debug { "Deleted: #{key}" }

        return unless @delete_subscribers

        # Subscribers to specific paths
        if subscribers = @delete_subscribers[key]
          subscribers.each{|subscriber| subscriber.call(key, revision)}
        end

        # Any subscribers for all events?
        if all_subscribers = @delete_subscribers['*']
          all_subscribers.each{|subscriber| subscriber.call(key, revision)}
        end
      end

    end
  end
end