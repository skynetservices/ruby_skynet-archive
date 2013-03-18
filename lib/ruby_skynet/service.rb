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
  module Service
    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        include SemanticLogger::Loggable

        sync_cattr_reader :logger do
          SemanticLogger::Logger.new(self)
        end
      end
      # Register the service with the Server
      # The server will publish the server to Doozer when the server is running
      Server.register_service(base)
    end

    module ClassMethods
      # Name of this service to Register with Skynet
      # Default: class name
      def service_name
        @@service_name ||= name.gsub('::', '.')
      end

      def service_name=(service_name)
        @@service_name = service_name
      end

      # Version of this service to register with Skynet, defaults to 1
      # Default: 1
      def service_version
        @@service_version ||= 1
      end

      def service_version=(service_version)
        @@service_version = service_version
      end

      # Key by which this service is known in the doozer registry
      def service_key
        "/services/#{service_name}/#{service_version}/#{Server.region}/#{Server.hostname}/#{Server.port}"
      end
    end

  end
end


