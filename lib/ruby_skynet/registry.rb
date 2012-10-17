require 'sync_attr'
require 'multi_json'
require 'thread_safe'

#
# RubySkynet Registry Client
#
# Keeps a local copy of the Skynet Registry
#
# Subscribes to Registry changes and the internal copy up to date
#
module RubySkynet
  class Registry
    include SyncAttr

    # Service Registry has the following format
    #  Key: [String] 'service_name/version/region'
    #  Value: [Array<String>] 'host:port', 'host:port'
    sync_cattr_accessor :service_registry do
      start_monitoring
    end

    @@on_server_removed_callbacks = ThreadSafe::Hash.new
    @@monitor_thread = nil

    DOOZER_SERVICES_PATH = "/services/*/*/*/*/*"

    # Default doozer configuration
    # To replace this default, set the config as follows:
    #    RubySkynet::Client.doozer_config = { .... }
    #
    #   :servers [Array of String]
    #     Array of URL's of doozer servers to connect to with port numbers
    #     ['server1:2000', 'server2:2000']
    #
    #     The second server will only be attempted once the first server
    #     cannot be connected to or has timed out on connect
    #     A read failure or timeout will not result in switching to the second
    #     server, only a connection failure or during an automatic reconnect
    #
    #   :read_timeout [Float]
    #     Time in seconds to timeout on read
    #     Can be overridden by supplying a timeout in the read call
    #
    #   :connect_timeout [Float]
    #     Time in seconds to timeout when trying to connect to the server
    #
    #   :connect_retry_count [Fixnum]
    #     Number of times to retry connecting when a connection fails
    #
    #   :connect_retry_interval [Float]
    #     Number of seconds between connection retry attempts after the first failed attempt
    sync_cattr_accessor :doozer_config do
      {
        :servers                => ['127.0.0.1:8046'],
        :read_timeout           => 5,
        :connect_timeout        => 3,
        :connect_retry_interval => 1,
        :connect_retry_count    => 300
      }
    end

    # Lazy initialize Doozer Client
    sync_cattr_reader :doozer do
      Doozer::Client.new(doozer_config)
    end

    # Logging instance for this class
    sync_cattr_reader :logger do
      SemanticLogger::Logger.new(self)
    end

    # Returns [Array] of the hostname and port pair [String] that implements a particular service
    # Performs a doozer lookup to find the servers
    #
    #   service_name:
    #     Name of the service to lookup
    #   version:
    #     Version of service to locate
    #     Default: All versions
    #   region:
    #     Region to look for the service in
    def self.registered_implementers(service_name='*', version='*', region='Development')
      hosts = []
      doozer.walk("/services/#{service_name}/#{version}/#{region}/*/*").each do |node|
        entry = MultiJson.load(node.value)
        hosts << entry if entry['Registered']
      end
      hosts
    end

    # Returns [Array<String>] a list of servers implementing the requested service
    def self.servers_for(service_name, version='*', region='Development', remote = false)
      if remote
        if version != '*'
          registered_implementers(service_name, version, region).map do |host|
            service = host['Config']['ServiceAddr']
            "#{service['IPAddress']}:#{service['Port']}"
          end
        else
          # Find the highest version of any particular service
          versions = {}
          registered_implementers(service_name, version, region).each do |host|
            service = host['Config']['ServiceAddr']
            (versions[version.to_i] ||= []) << "#{service['IPAddress']}:#{service['Port']}"
          end
          # Return the servers implementing the highest version number
          versions.sort.last.last
        end
      else
        if version == '*'
          # Find the highest version for the named service in this region
          version = -1
          service_registry.keys.each do |key|
            if match = key.match(/#{service_name}\/(\d+)\/#{region}/)
              ver = match[1].to_i
              version = ver if ver > version
            end
          end
        end
        service_registry["#{service_name}/#{version}/#{region}"]
      end
    end

    # Invokes registered callbacks when a specific server is shutdown or terminates
    # Not when a server de-registers itself
    # The callback will only be called once and will need to be re-registered
    # after being called if future callbacks are required for that server
    def self.on_server_removed(server, &block)
      (@@on_server_removed_callbacks[server] ||= ThreadSafe::Array.new) << block
    end

    ############################
    protected

    # Fetch the all registry information from Doozer and set the internal registry
    # Also starts the monitoring thread to keep the registry up to date
    def self.start_monitoring
      registry = ThreadSafe::Hash.new
      revision = doozer.current_revision
      doozer.walk(DOOZER_SERVICES_PATH, revision).each do |node|
        # path: "/services/TutorialService/1/Development/127.0.0.1/9000"
        e = node.path.split('/')

        # Key: [String] 'service_name/version/region'
        key = "#{e[2]}/#{e[3]}/#{e[4]}"
        server = "#{e[5]}:#{e[6]}"

        if node.value.strip.size > 0
          entry = MultiJson.load(node.value)
          if entry['Registered']
            # Value: [Array<String>] 'host:port', 'host:port'
            servers = (registry[key] ||= ThreadSafe::Array.new)
            servers << server unless servers.include?(server)
            logger.trace "#monitor Add/Update Service: #{key} => #{server}"
          end
        end
      end
      # Start monitoring thread to keep the registry up to date
      @@monitor_thread = Thread.new { watch(revision + 1) }
      registry
    end

    # Waits for any updates from Doozer and updates the internal service registry
    def self.watch(revision)
      logger.info "Start monitoring #{DOOZER_SERVICES_PATH}"
      doozer.watch(DOOZER_SERVICES_PATH, revision) do |node|
        # path: "/services/TutorialService/1/Development/127.0.0.1/9000"
        e = node.path.split('/')

        # Key: [String] 'service_name/version/region'
        key = "#{e[2]}/#{e[3]}/#{e[4]}"
        server = "#{e[5]}:#{e[6]}"

        if node.value.strip.size > 0
          entry = MultiJson.load(node.value)
          if entry['Registered']
            # Value: [Array<String>] 'host:port', 'host:port'
            servers = (service_registry[key] ||= ThreadSafe::Array.new)
            servers << server unless servers.include?(server)
            logger.trace "#monitor Add/Update Service: #{key} => #{server}"
          else
            logger.trace "#monitor Service deregistered, remove: #{key} => #{server}"
            if service_registry[key]
              service_registry[key].delete(server)
              service_registry.delete(key) if service_registry[key].size == 0
            end
          end
        else
          # Service has stopped and needs to be removed
          logger.trace "#monitor Service stopped, remove: #{key} => #{server}"
          if service_registry[key]
            service_registry[key].delete(server)
            service_registry.delete(key) if service_registry[key].size == 0
            server_removed(server)
          end
        end
        logger.trace "Updated registry", service_registry
      end
    ensure
      logger.info "Stopped monitoring"
    end

    # Invoke any registered callbacks for the specific server
    def self.server_removed(server)
      if callbacks = @@on_server_removed_callbacks.delete(server)
        callbacks.each do |block|
          begin
            block.call(server)
          rescue Exception => exc
            logger.error "Exception during a callback for server: #{server}", exc
          end
        end
      end
    end

  end
end
