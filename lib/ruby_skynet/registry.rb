require 'thread_safe'
require 'gene_pool'
require 'semantic_logger'
require 'sync_attr'
require 'ruby_doozer/json/deserializer'

#
# Registry
#
# Store information in doozer and subscribe to future changes
#
# Notifies registered subscribers when information has changed
#
# All paths specified are relative to the root. As such the root key
# is never returned, nor is it required when a key is supplied as input.
# For example, with a root of /foo/bar, any paths passed in will leave
# out the root: host/name
#
module RubyDoozer
  class Registry
    include SyncAttr
    # Logging instance for this class
    include SemanticLogger::Loggable

    attr_reader :doozer_config, :doozer_pool, :current_revision, :root

    # Create a Registry instance to manage a information within doozer
    #
    # :root [String]
    #   Root key to load and then monitor for changes
    #   It is not recommended to set the root to "/" as it will generate
    #   significant traffic since it will also monitor Doozer Admin changes
    #   Mandatory
    #
    # :doozer [Hash]
    #   Doozer configuration information
    #
    #   :servers [Array of String]
    #     Array of URL's of doozer servers to connect to with port numbers
    #     ['server1:2000', 'server2:2000']
    #
    #     An attempt will be made to connect to alternative servers when the
    #     current server cannot be connected to
    #     Default: ['127.0.0.1:8046']
    #
    #   :read_timeout [Float]
    #     Time in seconds to timeout on read
    #     Can be overridden by supplying a timeout in the read call
    #     Default: 5
    #
    #   :connect_timeout [Float]
    #     Time in seconds to timeout when trying to connect to the server
    #     Default: 3
    #
    #   :connect_retry_count [Fixnum]
    #     Number of times to retry connecting when a connection fails
    #     Default: 10
    #
    #   :connect_retry_interval [Float]
    #     Number of seconds between connection retry attempts after the first failed attempt
    #     Default: 0.5
    #
    #   :server_selector [Symbol|Proc]
    #     When multiple servers are supplied using :servers, this option will
    #     determine which server is selected from the list
    #       :ordered
    #         Select a server in the order supplied in the array, with the first
    #         having the highest priority. The second server will only be connected
    #         to if the first server is unreachable
    #       :random
    #         Randomly select a server from the list every time a connection
    #         is established, including during automatic connection recovery.
    #       Proc:
    #         When a Proc is supplied, it will be called passing in the list
    #         of servers. The Proc must return one server name
    #           Example:
    #             :server_selector => Proc.new do |servers|
    #               servers.last
    #             end
    #       Default: :random
    #
    #   :pool_size [Integer]
    #     Maximum size of the connection pool to doozer
    #     Default: 10
    #
    def initialize(params)
      params = params.dup
      @root = params.delete(:root) || params.delete(:root_path)
      raise "Missing mandatory parameter :root" unless @root

      # Add leading '/' to root if missing
      @root = "/#{@root}" unless @root.start_with?('/')

      # Strip trailing '/' if supplied
      @root = @root[0..-2] if @root.end_with?("/")
      @root_with_trail = "#{@root}/"

      @doozer_config = params.delete(:doozer) || {}
      @doozer_config[:servers]                ||= ['127.0.0.1:8046']
      @doozer_config[:read_timeout]           ||= 5
      @doozer_config[:connect_timeout]        ||= 3
      @doozer_config[:connect_retry_interval] ||= 0.5
      @doozer_config[:connect_retry_count]    ||= 10
      @doozer_config[:server_selector]        ||= :random

      # Allow the serializer and deserializer implementations to be replaced
      @serializer   = params.delete(:serializer)   || RubyDoozer::Json::Serializer
      @deserializer = params.delete(:deserializer) || RubyDoozer::Json::Deserializer

      # Connection pool settings
      @doozer_pool = GenePool.new(
        :name         =>"Doozer Connection Pool",
        :pool_size    => @doozer_config.delete(:pool_size) || 10,
        :timeout      => @doozer_config.delete(:pool_timeout) || 30,
        :warn_timeout => @doozer_config.delete(:pool_warn_timeout) || 5,
        :idle_timeout => @doozer_config.delete(:pool_idle_timeout) || 600,
        :logger       => logger,
        :close_proc   => :close
      ) do
        RubyDoozer::Client.new(@doozer_config)
      end

      # Generate warning log entries for any unknown configuration options
      params.each_pair {|k,v| logger.warn "Ignoring unknown configuration option: #{k}"}
    end

    # Start callback monitoring thread
    sync_attr_reader :monitor_thread do
      Thread.new { watch_registry }
    end

    # Retrieve the latest value from a specific path from the registry
    def [](key)
      value = doozer_pool.with_connection do |doozer|
        doozer[full_key(key)]
      end
      @deserializer.deserialize(value)
    end

    # Replace the latest value at a specific key
    def []=(key,value)
      doozer_pool.with_connection do |doozer|
        doozer[full_key(key)] = @serializer.serialize(value)
      end
    end

    # Delete the value at a specific key
    def delete(key)
      doozer_pool.with_connection do |doozer|
        doozer.delete(full_key(key))
      end
    end

    # Iterate over every key, value pair in the registry at the root
    #
    # If :cache was set to false on the initializer this call will
    # make network calls to doozer to retrieve the current values
    # Otherwise it is an in memory call against a duplicate of the registry
    #
    # Example:
    #   registry.each_pair {|k,v| puts "#{k} => #{v}"}
    def each_pair(&block)
      key = "#{@root}/**"
      doozer_pool.with_connection do |doozer|
        doozer.walk(key) do |key, value, revision|
          block.call(relative_key(key), @deserializer.deserialize(value))
        end
      end
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
      logger.info "Finalizing"
      if @monitor_thread
        @monitor_thread.kill
        @monitor_thread = nil
      end
      @doozer_pool.close if @doozer_pool
      @doozer_pool = nil
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
    #   registry.on_update do |key, value, revision|
    #     puts "#{key} was updated to #{value}"
    #   end
    def on_update(key='*', &block)
      # Start monitoring thread if not already started
      monitor_thread
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
      # Start monitoring thread if not already started
      monitor_thread
      ((@delete_subscribers ||= ThreadSafe::Hash.new)[key] ||= ThreadSafe::Array.new) << block
    end

    ############################
    protected

    # Returns the full key given a relative key
    def full_key(relative_key)
      "#{@root}/#{relative_key}"
    end

    # Returns the full key given a relative key
    def relative_key(full_key)
      full_key.sub(@root_with_trail, '')
    end

    # The key has been added or updated in the registry
    def changed(key, value, revision)
      logger.debug "Updated: #{key}", value

      return unless @update_subscribers

      # Subscribers to specific paths
      if subscribers = @update_subscribers[key]
        subscribers.each{|subscriber| subscriber.call(key, value, revision)}
      end

      # Any subscribers for all events?
      if all_subscribers = @update_subscribers['*']
        all_subscribers.each{|subscriber| subscriber.call(key, value, revision)}
      end
    end

    # Existing data has been removed from the registry
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

    # Waits for any updates from Doozer and updates the internal service registry
    def watch_registry
      watch_path = "#{@root}/**"
      logger.info "Start monitoring #{watch_path}"
      # This thread must use its own dedicated doozer connection
      doozer = RubyDoozer::Client.new(@doozer_config)
      @current_revision ||= doozer.current_revision

      # Watch for any new changes
      logger.debug "Monitoring thread started. Waiting for Registry Changes"
      doozer.watch(watch_path, @current_revision + 1) do |node|
        logger.trace "Registry Change Notification", node

        # Update the current_revision with every change notification
        @current_revision = node.rev

        # Remove the Root key
        key = relative_key(node.path)

        case node.flags
        when 4
          changed(key,  @deserializer.deserialize(node.value), node.rev)
        when 8
          deleted(key, node.rev)
        else
          logger.error "Unknown flags returned by doozer:#{node.flags}"
        end
      end
      logger.info "Stopping monitoring thread normally"

    rescue ScriptError, NameError, StandardError, Exception => exc
      logger.error "Exception in monitoring thread", exc
    ensure
      doozer.close if doozer
      logger.info "Stopped monitoring for changes in the doozer registry"
    end

  end
end
