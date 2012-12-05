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
    @@server   = nil

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

    @@services = ThreadSafe::Hash.new

    # Services currently loaded and available at this server when running
    def self.services
      @@services
    end

    # Registers a Service Class as being available at this port
    def self.register_service(klass)
      logger.debug "Registering Service: #{klass.name} with name: #{klass.service_name}"
      @@services[klass.service_name] = klass
      register_service_in_doozer(klass) if running?
    end

    # De-register service in doozer
    def self.deregister_service(klass)
      RubySkynet::Registry.doozer_pool.with_connection do |doozer|
        doozer.delete(klass.service_key) rescue nil
      end
      @@services.delete(klass.service_name)
    end

    # Returns whether the server is running
    def self.running?
      (@@server != nil) && @@server.running?
    end

    # Start the Server
    def self.start
      @@server = new
      @@server.start
    end

    # Stop the Server
    def self.stop
      @@server.terminate
      @@server = nil
    end

    def finalize
      @server.close if @server
      logger.info "Skynet Server Stopped"

      # Deregister services hosted by this server
      RubySkynet::Registry.doozer_pool.with_connection do |doozer|
        self.class.services.each_value do |klass|
          doozer.delete(klass.service_key) rescue nil
        end
      end
      logger.info "Skynet Services De-registered in Doozer"
    end

    # Returns whether the server is running
    def running?
      (@server != nil) && !@server.closed?
    end

    ############################################################################
    protected

    attr_accessor :server

    # Register the supplied service in doozer
    def self.register_service_in_doozer(klass)
      config = {
        "Config" => {
          "UUID"    => "#{Server.hostname}:#{Server.port}-#{$$}-#{klass.name}-#{klass.object_id}",
          "Name"    => klass.service_name,
          "Version" => klass.service_version.to_s,
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
        doozer[klass.service_key] = MultiJson.encode(config)
      end
    end

    # Start the server so that it can start taking RPC calls
    # Returns false if the server is already running
    def start
      return false if running?

      # Since we included Celluloid::IO, we're actually making a
      # Celluloid::IO::TCPServer here
      # TODO If port is in use, try the next port in sequence
      # TODO make port to listen on configurable
      @server = TCPServer.new('0.0.0.0', self.class.port)
      run!

      # Register services hosted by this server
      self.class.services.each_pair {|key, klass| self.class.register_service_in_doozer(klass)}
      true
    end

    def run
      logger.info("Starting listener on #{self.class.hostname}:#{self.class.port}")
      loop do
        logger.debug "Waiting for a client to connect"
        begin
          handle_connection!(@server.accept)
        rescue Exception => exc
          logger.error "Exception while processing connection request", exc
        end
      end
    end

    # Called for each message received from the client
    # Returns a Hash that is sent back to the caller
    def on_message(service_name, method, params)
      logger.benchmark_debug "Called: #{service_name}##{method}" do
        logger.trace "Method Call: #{method} with parameters:", params
        klass = Server.services[service_name]
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
