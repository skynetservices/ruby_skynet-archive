require 'bson'
#
# RubySkynet Client
#
# Supports
#   RPC calls to Skynet
#   Skynet Service autodiscovery
#
module RubySkynet
  class Client
    include Base

    attr_reader :skynet_name, :skynet_version, :skynet_region

    # Version of the Skynet service to use
    # By default it will connect to the latest version
    # Default: '*'
    def self.skynet_version
      @skynet_version ||= '*'
    end

    # Returns a new RubySkynet Client for the named service
    #
    # Calls to an instance of the Client are thread-safe and can be called
    # concurrently from multiple threads at the same time
    #
    # Parameters:
    #   :skynet_name
    #     Only required when creating instance of RubySkynet::Client directly
    #     Otherwise it defaults to the name of the class
    #     Name of the service to look for and connect to on Skynet
    #
    #   :skynet_version
    #     Optional version number of the service in Skynet
    #     Default: '*' being the latest version of the service
    #
    #   :skynet_region
    #     Optional region for this service in Skynet
    #     Default: RubySkynet.region
    #
    # Example using Client class
    #
    #  require 'ruby_skynet'
    #  SemanticLogger.default_level = :info
    #  SemanticLogger.add_appender(STDOUT)
    #
    #  class EchoService < RubySkynet::Client
    #  end
    #
    #  echo_service = EchoService.new
    #  p echo_service.echo(:value => 5)
    #
    # Example using Ruby Client directly
    #
    #  require 'ruby_skynet'
    #  SemanticLogger.default_level = :info
    #  SemanticLogger.add_appender(STDOUT)
    #
    #  tutorial_service = RubySkynet::Client.new('TutorialService')
    #  p tutorial_service.call('Add', :value => 5)
    def initialize(skynet_name=self.class.skynet_name, skynet_version=self.class.skynet_version, skynet_region=self.class.skynet_region)
      @skynet_name    = skynet_name
      @skynet_version = skynet_version
      @skynet_region  = skynet_region
      self.logger = SemanticLogger["#{self.class.name}: #{@skynet_name}/#{@skynet_version}/#{@skynet_region}"]

      raise "skynet_name is mandatory when using RubySkynet::Client directly" if @skynet_name == RubySkynet::Client.name
    end

    # Performs a synchronous call to the Skynet Service
    #
    # Parameters:
    #   method_name [String|Symbol]:
    #     Name of the method to call at the service
    #   parameters [Hash]:
    #     Parameters to pass into the service
    #
    # Returns the Hash result returned from the Skynet Service
    #
    # Raises RubySkynet::ProtocolError
    # Raises RubySkynet::SkynetException
    def call(method_name, parameters, connection_params={})
      # Skynet requires BSON RPC Calls to have the following format:
      # https://github.com/skynetservices/skynet/blob/master/protocol.md
      request_id = BSON::ObjectId.new.to_s

      # Obtain list of servers implementing this service in order of priority
      servers = ::RubySkynet.service_registry.servers_for(skynet_name, skynet_version, skynet_region)

      logger.tagged request_id do
        logger.benchmark_info "Called Skynet Service: #{skynet_name}.#{method_name}" do
          Connection.with_connection(servers, connection_params) do |connection|
            connection.rpc_call(request_id, skynet_name, method_name, parameters)
          end
        end
      end
    end

    # Implement methods that call the remote Service
    def method_missing(method, *args, &block)
      result = call(method, *args)

      # #TODO if Service returns method undefined, call super
      #
      # Define the method if the call was successful and no other thread has
      # already created the method
      if result[:exception].nil? && !self.class.method_defined?(method)
        self.class.send(:define_method, method) {|*args| call(method, *args)}
      end
      result
    end

  end
end
