require 'semantic_logger'
require 'thread_safe'

#
# RubySkynet Sevices Registry
#
# Loads a list of all services and which servers they are available on from a
# static YAML file
#
# Format of the YAML file
#   key: [String] "<name>/<version>/<region>"
#   value: [Array<String>] 'host:port', 'host:port'
#
module RubySkynet
  class StaticServiceRegistry
    include SemanticLogger::Loggable

    # Create a service registry
    def initialize(params = {})
      @services = params[:registry]
      raise "Missing :registry in config that must list the availables services" unless @services
    end

    # Returns the Service Registry as a Hash
    def to_h
      @services.dup
    end

    # Register the supplied service at this Skynet Server host and Port
    # Returns the UUID for the service that was created
    def register_service(name, version, region, hostname, port)
      server = "#{hostname}:#{port}"
      key = "#{name}/#{version}/#{region}"
      (@services[key] ||= []) << server
      key
    end

    # Deregister the supplied service from the Registry
    def deregister_service(name, version, region, hostname, port)
      server = "#{hostname}:#{port}"
      key = "#{name}/#{version}/#{region}"
      if servers = @services[key]
        servers.delete_if {|s| s == server}
        @services.delete(key) if servers.count == 0
      end
      key
    end

    # Returns [Array<String>] a list of servers implementing the requested service
    def servers_for(name, version='*', region=RubySkynet.region)
      if version == '*'
        # Find the highest version for the named service in this region
        version = -1
        @services.keys.each do |key|
          if match = key.match(/#{name}\/(\d+)\/#{region}/)
            ver = match[1].to_i
            version = ver if ver > version
          end
        end
      end
      servers = @services["#{name}/#{version}/#{region}"]
      raise ServiceUnavailable.new("No servers available for service: #{name} with version: #{version} in region: #{region}") unless servers
      servers
    end

    # Invokes registered callbacks when a specific server is shutdown or terminates
    # Not when a server de-registers itself
    # The callback will only be called once and will need to be re-registered
    # after being called if future callbacks are required for that server
    def on_server_removed(server, &block)
      #nop
    end

  end
end