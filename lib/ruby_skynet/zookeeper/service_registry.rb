require 'semantic_logger'
require 'thread_safe'
require 'gene_pool'
require 'resolv'

#
# RubySkynet Sevices Registry
#
# Based on the Skynet Services Registry, obtains and keeps up to date a list of
# all services and which servers they are available on.
#
module RubySkynet
  module Zookeeper
    class ServiceRegistry
      include SemanticLogger::Loggable

      # Create a service registry
      # See: RubyDoozer::Registry for the parameters
      def initialize
        # Registry has the following format
        #  Key: [String] 'name/version/region'
        #  Value: [Array<String>] 'host:port', 'host:port'
        @cache = ThreadSafe::Hash.new
        @notifications_cache = ThreadSafe::Hash.new

        # Supply block to load the current keys from the Registry
        @registry = Zookeeper::Registry.new(:root => '/instances') do |key, value|
          service_info_created(key, value)
        end
        # Register Callbacks
        @registry.on_create {|path, value| service_info_created(path, value) }
        @registry.on_update {|path, value| service_info_updated(path, value) }
        @registry.on_delete {|path|        service_info_deleted(path) }
      end

      # Returns the Service Registry as a Hash
      def to_h
        @cache.dup
      end

      # Register the supplied service at this Skynet Server host and Port
      # Returns the UUID for the service that was created
      def register_service(name, version, region, hostname, port)
        uuid = "#{hostname}:#{port}-#{$$}-#{name}-#{version}"
        # TODO Make sets ephemeral
        @registry[File.join(uuid,'addr')]       = "#{hostname}:#{port}"
        @registry[File.join(uuid,'name')]       = name
        @registry[File.join(uuid,'version')]    = version
        @registry[File.join(uuid,'region')]     = region
        @registry[File.join(uuid,'registered')] = true
	      uuid
      end

      # Deregister the supplied service from the Registry
      def deregister_service(name, version, region, hostname, port)
        uuid = "#{hostname}:#{port}-#{$$}-#{name}-#{version}"
        @registry.delete(File.join(uuid,'addr'), false)
        @registry.delete(File.join(uuid,'name'), false)
        @registry.delete(File.join(uuid,'version'), false)
        @registry.delete(File.join(uuid,'region'), false)
        @registry.delete(File.join(uuid,'registered'), false)
        @registry.delete(uuid, false)
      end

      # Return a server that implements the specified service
      def server_for(name, version='*', region=RubySkynet.region)
        if servers = servers_for(name, version, region)
          # Randomly select one of the servers offering the service
          servers[rand(servers.size)]
        else
          msg = "No servers available for service: #{name} with version: #{version} in region: #{region}"
          logger.warn msg
          raise ServiceUnavailable.new(msg)
        end
      end

      # Returns [Array<String>] a list of servers implementing the requested service
      def servers_for(name, version='*', region=RubySkynet.region)
        if version == '*'
          # Find the highest version for the named service in this region
          version = -1
          @cache.keys.each do |key|
            if match = key.match(/#{name}\/(\d+)\/#{region}/)
              ver = match[1].to_i
              version = ver if ver > version
            end
          end
        end
        if server_infos = @cache["#{name}/#{version}/#{region}"]
          server_infos.first.servers
        end
      end

      # Invokes registered callbacks when a specific server is shutdown or terminates
      # Not when a server de-registers itself
      # The callback will only be called once and will need to be re-registered
      # after being called if future callbacks are required for that server
      def on_server_removed(server, &block)
        ((@on_server_removed_callbacks ||= ThreadSafe::Hash.new)[server] ||= ThreadSafe::Array.new) << block
      end

      ############################
      protected

      def service_info_deleted(path)
        logger.info("service_info_deleted: #{path}")
        # path: "uuid/key"
        uuid, key = path.split('/')
        if (key == 'registered')
          if server = @notifications_cache.delete(uuid)
            hostname, port = server['addr'].split(':')
            # Service has stopped and needs to be removed
            remove_server(File.join(server['name'], server['version'].to_s, server['region']), hostname, port, true)
          end
        end
      end

      # Service information changed, so update internal registry
      def service_info_created(path, value=nil)
        logger.info("service_info_created: #{path}", value)
        # path: "uuid/key"
        uuid, key = path.split('/')

        # Big Assumption: 'registered' event will be received last as true
        if (key == 'registered') && (value == true)
          server = @notifications_cache[uuid]
          if server && server['addr'] && server['name'] && server['version'] && server['region']
            hostname, port = server['addr'].split(':')
            add_server(File.join(server['name'], server['version'].to_s, server['region']), hostname, port)
          else
            logger.warn "Registered notification received for #{uuid} but is missing critical information. Received:", server
          end
        else
          (@notifications_cache[uuid] ||= ThreadSafe::Hash.new)[key] = value
        end
      end

      def service_info_updated(path, value=nil)
        logger.info("service_info_updated: #{path}", value)
        # path: "uuid/key"
        uuid, key = path.split('/')

        # Big Assumption: 'registered' event will be received last as true
        if (key == 'registered') && (value == true)
          server = @notifications_cache[uuid]
          if server && server['addr'] && server['name'] && server['version'] && server['region']
            hostname, port = server['addr'].split(':')
            add_server(File.join(server['name'], server['version'].to_s, server['region']), hostname, port)
          else
            logger.warn "Registered notification received for #{uuid} but is missing critical information. Received:", server
          end
        else
          (@notifications_cache[uuid] ||= ThreadSafe::Hash.new)[key] = value
        end
      end

      # :score:   [Integer] Score
      # :servers: [Array<String>] 'host:port', 'host:port'
      ServerInfo = Struct.new(:score, :servers )

      # Format of the internal services registry
      #   key: [String] "<name>/<version>/<region>"
      #   value: [ServiceInfo, ServiceInfo]
      #          Sorted by highest score first

      # Add the host to the registry based on it's score
      def add_server(key, hostname, port)
        server  = "#{hostname}:#{port}"

        server_infos = (@cache[key] ||= ThreadSafe::Array.new)

        # If already present, then nothing to do
        server_info = server_infos.find{|si| si.servers.include?(server)}
        return server_info if server_info

        # Look for the same score with a different server
        score = self.class.score_for_server(hostname, RubySkynet.local_ip_address)
        logger.info "Service: #{key} now running at #{server} with score #{score}"
        if server_info = server_infos.find{|si| si.score == score}
          server_info.servers << server
          return server_info
        end

        # New score
        servers = ThreadSafe::Array.new
        servers << server
        server_info = ServerInfo.new(score, servers)

        # Insert into Array in order of score
        if index = server_infos.find_index {|si| si.score <= score}
          server_infos.insert(index, server_info)
        else
          server_infos << server_info
        end
        server_info
      end

      # Remove the host from the registry based
      # Returns the server instance if it was removed
      def remove_server(key, hostname, port, notify)
        server = "#{hostname}:#{port}"
        logger.info "Service: #{key} stopped running at #{server}"
        server_info = nil
        if server_infos = @cache[key]
          server_infos.each do |si|
            if si.servers.delete(server)
              server_info = si
              break
            end
          end

          # Found server
          if server_info
            # Cleanup if no more servers in server list
            server_infos.delete(server_info) if server_info.servers.size == 0

            # Cleanup if no more server infos
            @cache.delete(key) if server_infos.size == 0

            server_removed(server) if notify
          end
        end
        server_info
      end

      # Invoke any registered callbacks for the specific server
      def server_removed(server)
        if @on_server_removed_callbacks && (callbacks = @on_server_removed_callbacks.delete(server))
          callbacks.each do |block|
            begin
              logger.debug "Calling callback for server: #{server}"
              block.call(server)
            rescue Exception => exc
              logger.error("Exception during a callback for server: #{server}", exc)
            end
          end
        end
      end

      IPV4_REG_EXP = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/

      # Returns [Integer] the score for the supplied ip_address
      # Score currently ranges from 0 to 4 with 4 being the best score
      # If the IP address does not match an IP v4 address a DNS lookup will
      # be performed
      def self.score_for_server(ip_address, local_ip_address)
        ip_address = '127.0.0.1' if ip_address == 'localhost'
        score = 0
        # Each matching element adds 1 to the score
        # 192.168.  0.  0
        #               1
        #           1
        #       1
        #   1
        server_match = IPV4_REG_EXP.match(ip_address) || IPV4_REG_EXP.match(Resolv::DNS.new.getaddress(ip_address).to_s)
        if server_match
          local_match = IPV4_REG_EXP.match(local_ip_address)
          score = 0
          (1..4).each do |i|
            break if local_match[i].to_i != server_match[i].to_i
            score += 1
          end
        end
        score
      end

    end
  end
end