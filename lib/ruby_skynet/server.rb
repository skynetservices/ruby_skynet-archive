require 'bson'
require 'celluloid/io'

# Replace Celluloid logger immediately upon loading the Server Instance
Celluloid.logger = SemanticLogger::Logger.new('Celluloid')

#
# RubySkynet Server
#
# Hosts one or more Skynet Services
#
module RubySkynet
  class Server
    include Celluloid::IO
    include SemanticLogger::Loggable

    # TODO Make Server instance based rather than class based. Then make instance global
    @@hostname = nil
    @@port     = 2000
    @@region   = 'Development'

    # Region under which to register Skynet services
    # Default: 'Development'
    def self.region
      @@region
    end

    def self.region=(region)
      @@region = region
    end

    # Port to listen to requests on
    # Default: 2000
    def self.port
      @@port
    end

    def self.port=(port)
      @@port = port
    end

    # Override the hostname at which this server is running
    # Useful when the service is behind a firewall or NAT device
    def self.hostname=(hostname)
      @@hostname = hostname
    end

    # Returns [String] hostname of the current server
    def self.hostname
      @@hostname ||= Socket.gethostname
    end

    # Returns a new RubySkynet Server at the specified hostname and port
    #
    # Example
    #
    #  require 'ruby_skynet'
    #  SemanticLogger.default_level = :trace
    #  SemanticLogger.appenders << SemanticLogger::Appender::File(STDOUT)
    #
    #  server = RubySkynet::Server.new(2000)
    def initialize(listen_ip = '0.0.0.0')
      # Since we included Celluloid::IO, we're actually making a
      # Celluloid::IO::TCPServer here
      # TODO If port is in use, try the next port in sequence
      @server = TCPServer.new(listen_ip, self.class.port)
      run!
    end

    def run
      logger.info("Starting listener on #{self.class.hostname}:#{self.class.port}")
      loop do
        logger.debug "Waiting for a client to connect"
        handle_connection!(@server.accept)
      end
    end

    def finalize
      @server.close if @server
      logger.info "Skynet Server Stopped"
    end

    # Called for each message received from the client
    # Returns a Hash that is sent back to the caller
    def on_message(service_name, method, params)
      logger.benchmark_debug "Called: #{service_name}##{method}" do
        logger.trace "Method Call: #{method} with parameters:", params
        klass = Service.registered_services[service_name]
        raise "Invalid Skynet RPC call, service: #{service_name} is not available at this server" unless klass
        service = klass.new
        raise "Invalid Skynet RPC call, method: #{method} does not exist for service: #{service_name}" unless service.respond_to?(method)
        # TODO Use pool of services, or Celluloid here
        service.send(method, params)
      end
    end

    # Called for each client connection
    def handle_connection(client)
      logger.debug "Client connected, waiting for data from client"

      # Process handshake
      handshake = {
        'registered' => true,
        'clientid'   => BSON::ObjectId.new.to_s
      }
      client.write(BSON.serialize(handshake))
      Common.read_bson_document(client)

      while(header = Common.read_bson_document(client)) do
        logger.debug "\n******************"
        logger.debug "Received Request"
        logger.trace 'Header', header

        service_name = header['servicemethod']  # "#{service_name}.Forward",
        raise "Invalid Skynet RPC Request, missing servicemethod" unless service_name
        match = service_name.match /(.*)\.Forward$/
        raise "Invalid Skynet RPC Request, servicemethod must end with '.Forward'" unless match
        service_name = match[1]

        request = Common.read_bson_document(client)
        logger.trace 'Request', request
        break unless request
        params = BSON.deserialize(request['in'])
        logger.trace 'Parameters', params
        reply = begin
          on_message(service_name, request['method'].to_sym, params)
        rescue Exception => exc
          logger.error "Exception while calling service: #{service_name}", exc
          # TODO Return exception in header
          { :exception => {:message => exc.message, :class => exc.class.name} }
        end

        if reply
          logger.debug "Sending Header"
          # For this test we just send back the received header
          client.write(BSON.serialize(header))

          logger.debug "Sending Reply"
          logger.trace 'Reply', reply
          client.write(BSON.serialize({'out' => BSON.serialize(reply).to_s}))
        else
          logger.debug "Closing client since no reply is being sent back"
          break
        end
      end
      # Disconnect from the client
      client.close
      logger.debug "Disconnected from the client"
    end

  end
end
