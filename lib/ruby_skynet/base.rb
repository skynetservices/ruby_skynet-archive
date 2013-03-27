require 'semantic_logger'

# Base class for RubySkynet Clients and Services
module RubySkynet
  module Base
    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        include SemanticLogger::Loggable
        include InstanceMethods
      end
    end

    module InstanceMethods
      # Implement methods that call the remote Service
      def method_missing(method, *args, &block)
        puts "Methods: #{method}, #{args.inspect}"
        puts "Args Size=#{args.size}"
        result = ruby_skynet_client.call(method, *args)
        # Define the method if the call was successful and no-one else already
        # created the method
        if result[:exception].nil? && !self.class.method_defined?(method)
          self.class.send(:define_method, method) {|*args| ruby_skynet_client.call(method, *args)}
        end
        result
      end

      def ruby_skynet_client
        @ruby_skynet_client ||= RubySkynet::Client.new(self.class.skynet_name, self.class.skynet_version || '*', self.class.skynet_region)
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
