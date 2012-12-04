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
    # Returns a new RubySkynet Client for the named service
    #
    # Calls to an instance of the Client are thread-safe and can be called
    # concurrently from multiple threads at the same time
    #
    # Parameters:
    #   :service_name
    #     Name of the service to look for and connect to on Skynet
    #
    #   :version
    #     Optional version number of the service in Skynet
    #     Default: '*' being the latest version of the service
    #
    #   :region
    #     Optional region for this service in Skynet
    #     Default: 'Development'
    #
    # Example
    #
    #  require 'ruby_skynet'
    #  SemanticLogger.default_level = :trace
    #  SemanticLogger.appenders << SemanticLogger::Appender::File(STDOUT)
    #
    #  tutorial_service = RubySkynet::Client.new('TutorialService')
    #  p tutorial_service.call('Add', :value => 5)
    def initialize(service_name, version='*', region='Development')
      @service_name = service_name
      @logger       = SemanticLogger::Logger.new("#{self.class.name}: #{service_name}/#{version}/#{region}")
      @version      = version
      @region       = region
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
      # https://github.com/bketelsen/skynet/blob/protocol/protocol.md
      request_id = BSON::ObjectId.new.to_s
      @logger.tagged request_id do
        @logger.benchmark_info "Called Skynet Service: #{@service_name}.#{method_name}" do
          retries = 0
          # If it cannot connect to a server, try a different server
          begin
            Connection.with_connection(Registry.server_for(@service_name, @version, @region), connection_params) do |connection|
              connection.rpc_call(request_id, @service_name, method_name, parameters)
            end
          rescue ResilientSocket::ConnectionFailure => exc
            if (retries < 3) && exc.cause.is_a?(Errno::ECONNREFUSED)
              retries += 1
              retry
            end
            # TODO rescue ServiceUnavailable retry x times until the service becomes available
          end
        end
      end
    end

  end
end
