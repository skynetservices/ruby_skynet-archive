# Doozer entries are in json
require 'multi_json'
require 'thread_safe'

#
# RubySkynet Service
#
# Supports
#   Hosting Skynet Services
#   Skynet Service registration
#
module RubySkynet
  class Service
    include SemanticLogger::Loggable

    # Name of this service to Register with Skynet
    # Default: class name
    def self.service_name
      @@service_name ||= name.gsub('::', '.')
    end

    def self.service_name=(service_name)
      @@service_name = service_name
    end

    # Version of this service to register with Skynet, defaults to 1
    # Default: 1
    def self.service_version
      @@service_version ||= 1
    end

    def self.service_version=(service_version)
      @@service_version = service_version
    end

    @@services = ThreadSafe::Hash.new

    # Registers a Service Class as being available at this port
    def self.register_service
      @@services[service_name] = self
      config = {
        "Config" => {
          "UUID"    => "#{Server.hostname}:#{Server.port}-#{$$}-#{name}-#{object_id}",
          "Name"    => service_name,
          "Version" => service_version.to_s,
          "Region"  => Server.region,
          "ServiceAddr" => {
            "IPAddress" => Server.hostname,
            "Port"      => Server.port,
            "MaxPort"   => Server.port + 999
          },
        },
        "Registered" => true
      }
      RubySkynet::Registry.doozer_pool.with_connection do |doozer|
        doozer[service_key] = MultiJson.encode(config)
      end
    end

    # De-register service in doozer
    def self.deregister_service
      RubySkynet::Registry.doozer_pool.with_connection do |doozer|
        doozer.delete(service_key) rescue nil
      end
    end

    # Key by which this service is known in the doozer registry
    def self.service_key
      "/services/#{service_name}/#{service_version}/#{Server.region}/#{Server.hostname}/#{Server.port}"
    end

    def self.registered_services
      @@services
    end

    # Move to include
    register_service

    # Methods implemented by this service
    # Must take a Hash as input
    # Must Return a Hash response or nil for no response
    def echo(params)
      params
    end

  end
end


