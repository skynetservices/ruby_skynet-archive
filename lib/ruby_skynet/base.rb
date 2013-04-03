require 'semantic_logger'

# Base class for RubySkynet Clients and Services
module RubySkynet
  module Base
    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        include SemanticLogger::Loggable
      end
    end

    module ClassMethods
      # Name of this service to Register with Skynet
      # Default: class name
      def skynet_name
        @skynet_name ||= name.gsub('::', '.')
      end

      def skynet_name=(skynet_name)
        @skynet_name = skynet_name
      end

      # Version of this service to register with Skynet
      # Default: nil
      def skynet_version
        @skynet_version ||= nil
      end

      def skynet_version=(skynet_version)
        @skynet_version = skynet_version
      end

      # Region within which this service is defined
      # Default: RubySkynet.region
      def skynet_region
        @skynet_region || ::RubySkynet.region
      end

      def skynet_region=(skynet_region)
        @skynet_region = skynet_region
      end

    end

  end
end
