require 'sync_attr'
require 'multi_json'
require 'thread_safe'
require 'gene_pool'

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
    sync_cattr_reader :service_registry do
      start
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
        :connect_retry_count    => 30
      }
    end

    # Register the supplied service at this Skynet Server host and Port
    def self.register_service(klass, region, hostname, port)
      config = {
        "Config" => {
          "UUID"    => "#{hostname}:#{port}-#{$$}-#{klass.name}-#{klass.object_id}",
          "Name"    => klass.service_name,
          "Version" => klass.service_version.to_s,
          "Region"  => region,
          "ServiceAddr" => {
            "IPAddress" => hostname,
            "Port"      => port,
            "MaxPort"   => port + 999
          },
        },
        "Registered" => true
      }
      doozer_pool.with_connection do |doozer|
        doozer[klass.service_key] = MultiJson.encode(config)
      end
    end

    # Deregister the supplied service from the Registry
    def self.deregister_service(klass)
      doozer_pool.with_connection do |doozer|
        doozer.delete(klass.service_key) rescue nil
      end
    end

    # Return a server that implements the specified service
    def self.server_for(service_name, version='*', region='Development')
      if servers = servers_for(service_name, version, region)
        # Randomly select one of the servers offering the service
        servers[rand(servers.size)]
      else
        msg = "No servers available for service: #{service_name} with version: #{version} in region: #{region}"
        logger.warn msg
        raise ServiceUnavailable.new(msg)
      end
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
      doozer_pool.with_connection do |doozer|
        doozer.walk("/services/#{service_name}/#{version}/#{region}/*/*").each do |node|
          entry = MultiJson.load(node.value)
          hosts << entry if entry['Registered']
        end
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

    # Logging instance for this class
    sync_cattr_reader :logger do
      SemanticLogger::Logger.new(self, :debug)
    end

    #ServiceInfo = Struct.new(:service_name, :version, :region, :host, :port, :score)

    # Format of the internal services registry
    #   key: [String] "<service_name>/<version>/<region>"
    #   value: [ServiceInfo]

    # Lazy initialize Doozer Client Connection pool
    sync_cattr_reader :doozer_pool do
      GenePool.new(
        :name         =>"Doozer Connection Pool",
        :pool_size    => 5,
        :timeout      => 30,
        :warn_timeout => 5,
        :idle_timeout => 600,
        :logger       => logger,
        :close_proc   => :close
      ) do
        Doozer::Client.new(doozer_config)
      end
    end

    # Fetch the all registry information from Doozer and sets the internal registry
    # Also starts the monitoring thread to keep the registry up to date
    def self.start
      registry = ThreadSafe::Hash.new
      revision = nil
      doozer_pool.with_connection do |doozer|
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
              logger.debug "#start_monitoring Add Service: #{key} => #{server}"
            end
          end
        end
      end

      # Start monitoring thread to keep the registry up to date
      @@monitor_thread = Thread.new { self.watch(revision + 1) }

      # Cleanup when process exits
      at_exit do
        if @@monitor_thread
          @@monitor_thread.kill
          @@monitor_thread.join
          @@monitor_thread = nil
        end
        doozer_pool.close
      end
      registry
    end

    # Waits for any updates from Doozer and updates the internal service registry
    def self.watch(revision)
      logger.info "Start monitoring #{DOOZER_SERVICES_PATH}"
      # This thread must use its own dedicated doozer connection
      doozer = Doozer::Client.new(doozer_config)
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
            logger.debug "#monitor Add/Update Service: #{key} => #{server}"
          else
            logger.debug "#monitor Service deregistered, remove: #{key} => #{server}"
            if service_registry[key]
              service_registry[key].delete(server)
              service_registry.delete(key) if service_registry[key].size == 0
            end
          end
        else
          # Service has stopped and needs to be removed
          logger.debug "#monitor Service stopped, remove: #{key} => #{server}"
          if service_registry[key]
            service_registry[key].delete(server)
            service_registry.delete(key) if service_registry[key].size == 0
            server_removed(server)
          end
        end
        logger.debug "Updated registry", service_registry
      end
      logger.info "Stopping monitoring thread normally"
    rescue Exception => exc
      logger.error "Exception in monitoring thread", exc
    ensure
      doozer.close if doozer
      logger.info "Stopped monitoring"
    end

    # Invoke any registered callbacks for the specific server
    def self.server_removed(server)
      if callbacks = @@on_server_removed_callbacks.delete(server)
        callbacks.each do |block|
          begin
            logger.info "Calling callback for server: #{server}"
            block.call(server)
          rescue Exception => exc
            logger.error("Exception during a callback for server: #{server}", exc)
          end
        end
      end
    end

  end
end
