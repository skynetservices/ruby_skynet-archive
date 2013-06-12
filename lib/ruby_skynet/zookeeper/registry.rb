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
      def initialize(params)
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
      def finalize
        @zookeeper.close
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
      private

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

    end
  end
end