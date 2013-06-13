require 'thread_safe'
require 'semantic_logger'
require 'ruby_skynet/zookeeper/registry'

#
# CachedRegistry
#
# Store information in ZooKeeper and subscribe to future changes
# and keep a local copy of the information in ZooKeeper
#
# Notifies registered subscribers when information has changed
#
# All paths specified are relative to the root_path. As such the root key
# is never returned, nor is it required when a key is supplied as input.
# For example, with a root_path of /foo/bar, any paths passed in will leave
# out the root_path: host/name
#
# Keeps a local copy in memory of all descendant values of the supplied root_path
# Supports high-frequency calls to retrieve registry data
# The in-memory cache will be kept in synch with any changes on the server
module RubySkynet
  module Zookeeper
    class CachedRegistry < Registry
      # Logging instance for this class
      include SemanticLogger::Loggable

      # Create a CachedRegistry instance to manage information within the Registry
      # and keep a local cached copy of the data in the Registry to support
      # high-speed or frequent reads.
      #
      # Writes are sent to ZooKeeper and then replicated back to the local cache
      # only once ZooKeeper has updated its store
      #
      # See RubySkynet::Zookeeper::Registry for the complete list of options
      #
      def initialize(params)
        @cache = ThreadSafe::Hash.new
        # Supplied block to load the current keys from the Registry
        super(params) do |key, value, version|
          @cache[key] = value
        end

        on_create {|key, value|          @cache[key] = value}
        on_update {|key, value, version| @cache[key] = value}
        on_delete {|key|                 @cache.delete(key)}
      end

      # Retrieve the latest value from a specific key from the registry
      def [](key)
        @cache[key]
      end

      # Iterate over every key, value pair in the registry at the root_path
      #
      # Example:
      #   registry.each_pair {|k,v| puts "#{k} => #{v}"}
      def each_pair(&block)
        # Have to duplicate the cache otherwise concurrent changes to the
        # registry will interfere with the iterator
        @cache.dup.each_pair(&block)
      end

      # Returns [Array<String>] all keys in the registry
      def keys
        @cache.keys
      end

      # Returns a copy of the registry as a Hash
      def to_h
        @cache.dup
      end

    end
  end
end