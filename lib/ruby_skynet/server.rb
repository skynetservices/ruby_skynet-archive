require 'bson'

#
# RubySkynet Server
#
# Hosts one or more Skynet Services
#
module RubySkynet
  class Server
    include SemanticLogger::Loggable

    @@server   = nil
    @@services = ThreadSafe::Hash.new

    # Start a single instance of the server
    def self.start(region = 'Development', start_port = 2000, hostname = nil)
      @@server ||= new(region, start_port, hostname)
    end

    # Stop the single instance of the server
    def self.stop
      @@server.finalize if @@server
      @@server = nil
    end

    # Is the single instance of the server running
    def self.running?
      (@@server != nil) && @@server.running?
    end

    # Services currently loaded and available at this server when running
    def self.services
      @@services
    end

    # Registers a Service Class as being available at this host and port
    def self.register_service(klass)
      raise InvalidServiceException.new("#{klass.inspect} is not a RubySkynet::Service") unless klass.respond_to?(:service_name) && klass.respond_to?(:service_version)

      if previous_klass = @@services[klass.service_name] && (previous_klass.name != klass.name)
        logger.warn("Service with name: #{klass.service_name} is already registered to a different implementation:#{previous_klass.name}")
      end
      @@services[klass.service_name] = klass
      @@server.register_service(klass) if @@server
    end

    # De-register service
    def self.deregister_service(klass)
      raise InvalidServiceException.new("#{klass.inspect} is not a RubySkynet::Service") unless klass.respond_to?(:service_name) && klass.respond_to?(:service_version)

      @@server.deregister_service(klass) if @@server
      @@services.delete(klass.service_name)
    end

    # The actual port the server is running at which will be different
    # from Server.port if that port was already in use at startup
    attr_reader :hostname, :port, :region

    # Start the server so that it can start taking RPC calls
    # Returns false if the server is already running
    def initialize(region = 'Development', start_port = 2000, hostname = nil)
      hostname ||= Common.local_ip_address

      # If port is in use, try the next port in sequence
      port_count = 0
      begin
        @server   = ::TCPServer.new(hostname, start_port + port_count)
        @hostname = hostname
        @port     = start_port + port_count
        @region   = region
      rescue Errno::EADDRINUSE => exc
        if port_count < 999
          port_count += 1
          retry
        end
        raise exc
      end

      # Start Server listener thread
      Thread.new { run }

      # Register services hosted by this server
      self.class.services.each_value {|klass| register_service(klass)}
    end

    def finalize
      @server.close if @server
      logger.info "Skynet Server Stopped"

      # Deregister services hosted by this server
      self.class.services.each_value do |klass|
        deregister_service(klass) rescue nil
      end
      logger.info "Skynet Services De-registered"
    end

    def run
      logger.info("Starting listener on #{hostname}:#{port}")
      loop do
        logger.debug "Waiting for a client to connect"
        begin
          client = @server.accept
          # We could use a thread pool here, but JRuby already does that
          # and MRI threads are very light weight
          Thread.new { handle_connection(client) }
        rescue Errno::EBADF, IOError => exc
          logger.info "TCPServer listener thread shutting down. #{exc.class}: #{exc.message}"
          return
        rescue ScriptError, NameError, StandardError, Exception => exc
          logger.error "Exception while processing connection request", exc
        end
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
      client.write(BSON.serialize(handshake).to_s)
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
        rescue ScriptError, NameError, StandardError, Exception => exc
          logger.error "Exception while calling service: #{service_name}", exc
          { :exception => {:message => exc.message, :class => exc.class.name} }
        end

        if reply
          logger.debug "Sending Header"
          # For this test we just send back the received header
          client.write(BSON.serialize(header).to_s)

          logger.debug "Sending Reply"
          logger.trace 'Reply', reply
          client.write(BSON.serialize('out' => BSON::Binary.new(BSON.serialize(reply))).to_s)
        else
          logger.debug "Closing client since no reply is being sent back"
          break
        end
      end
    rescue ScriptError, NameError, StandardError, Exception => exc
      logger.error "#handle_connection Exception", exc
    ensure
      # Disconnect from the client
      client.close
      logger.debug "Disconnected from the client"
    end

    # Returns whether the server is running
    def running?
      (@server != nil) && !@server.closed?
    end

    ############################################################################
    protected

    # Registers a Service Class as being available at this server
    def register_service(klass)
      logger.debug "Registering Service: #{klass.name} with name: #{klass.service_name}"
      Registry.register_service(klass.service_name, klass.service_version, @region, @hostname, @port)
    end

    # De-register service from this server
    def deregister_service(klass)
      logger.debug "De-registering Service: #{klass.name} with name: #{klass.service_name}"
      Registry.deregister_service(klass.service_name, klass.service_version, @region, @hostname, @port)
    end

    # Called for each message received from the client
    # Returns a Hash that is sent back to the caller
    def on_message(service_name, method, params)
      logger.benchmark_info("Skynet Call: #{service_name}##{method}") do
        logger.trace "Method Call: #{method} with parameters:", params
        klass = Server.services[service_name]
        raise "Invalid Skynet RPC call, service: #{service_name} is not available at this server" unless klass
        # TODO Use pool of services
        service = klass.new
        raise "Invalid Skynet RPC call, method: #{method} does not exist for service: #{service_name}" unless service.respond_to?(method)
        service.send(method, params)
      end
    end

  end
end
