require 'thread_safe'

#
# RubySkynet Server
#
# Hosts one or more Skynet Services
#
module RubySkynet
  class Server
    include SemanticLogger::Loggable

    # The actual port the server is running at
    attr_reader :hostname, :port, :services

    # Start the server so that it can start taking RPC calls
    # Returns false if the server is already running
    def initialize(start_port = nil, ip_address = nil)
      ip_address ||= RubySkynet.local_ip_address
      start_port = (start_port || RubySkynet.server_port).to_i
      raise InvalidConfigurationException.new("Invalid Starting Port number: #{start_port}") unless start_port > 0

      # If port is in use, try the next port in sequence
      port_count = 0
      begin
        @server   = ::TCPServer.new(ip_address, start_port + port_count)
        @hostname = ip_address
        @port     = start_port + port_count
      rescue Errno::EADDRINUSE => exc
        if port_count < 999
          port_count += 1
          retry
        end
        raise exc
      end

      # Start Server listener thread
      @listener_thread = Thread.new { run }

      # Array[RubySkynet::Service] List of services registered with this server instance
      @services = ThreadSafe::Hash.new
    end

    def close
      @server.close if @server
      logger.info "Skynet Server Stopped"

      # Deregister services hosted by this server
      @services.each_value do |klass|
        deregister_service(klass) rescue nil
      end
      logger.info "Skynet Services De-registered"
    end

    # Returns whether the server is running
    def running?
      (@server != nil) && !@server.closed?
    end

    # Wait forever until the running server stops
    def wait_until_server_stops
      @listener_thread.join
    end

    # Registers a Service Class as being available at this server
    def register_service(klass)
      raise InvalidServiceException.new("#{klass.inspect} is not a RubySkynet::Service") unless klass.respond_to?(:skynet_name) && klass.respond_to?(:skynet_version) && klass.respond_to?(:skynet_region)

      previous_klass = @services[klass.skynet_name]
      if previous_klass && (previous_klass.name != klass.name)
        logger.warn("Service with name: #{klass.skynet_name} is already registered to a different implementation:#{previous_klass.name}")
      end
      @services[klass.skynet_name] = klass

      logger.info "Registering Service: #{klass.name} with name: #{klass.skynet_name}"
      ::RubySkynet.service_registry.register_service(klass.skynet_name, klass.skynet_version || 1, klass.skynet_region, @hostname, @port)
    end

    # De-register service from this server
    def deregister_service(klass)
      raise InvalidServiceException.new("#{klass.inspect} is not a RubySkynet::Service") unless klass.respond_to?(:skynet_name) && klass.respond_to?(:skynet_version) && klass.respond_to?(:skynet_region)

      logger.info "De-registering Service: #{klass.name} with name: #{klass.skynet_name}"
      ::RubySkynet.service_registry.deregister_service(klass.skynet_name, klass.skynet_version || 1, klass.skynet_region, @hostname, @port)
      @services.delete(klass.skynet_name)
    end

    # Loads and registers all services found in the supplied path and it's sub-directories
    # Returns [RubySkynet::Service] the list of Services registered
    def register_services_in_path(path=RubySkynet.services_path)
      logger.benchmark_info "Loaded Skynet Services" do
        # Load services
        klasses = []
        Dir.glob("#{path}/**/*.rb").each do |filename|
          partial = filename.sub(path,'').sub('.rb', '')
          load filename
          camelized = partial.gsub(/\/(.?)/) { "::#{$1.upcase}" }.gsub(/(?:^|_)(.)/) { $1.upcase }
          begin
            klass = constantize(camelized)
            # Register the service
            register_service(klass)
            klasses << klass
          rescue Exception => exc
            p exc
            raise "Expected to find class #{camelized} in file #{filename}"
          end
        end
        klasses
      end
    end

    ############################################################################
    protected

    # Re-Register services hosted by this server in the registry
    def re_register_services_in_registry
      @services.each_value {|klass| register_service(klass)}
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
      client.write(handshake.to_bson)
      Common.read_bson_document(client)

      while(header = Common.read_bson_document(client)) do
        logger.debug "\n******************"
        logger.debug "Received Request"
        logger.trace 'Header', header

        name = header['servicemethod']
        raise "Invalid Skynet RPC Request, missing servicemethod" unless name
        match = name.match /(.*)\.Forward$/
        raise "Invalid Skynet RPC Request, servicemethod must end with '.Forward'" unless match
        name = match[1]

        request = Common.read_bson_document(client)
        logger.trace 'Request', request
        break unless request
        params = Hash.from_bson(StringIO.new(request['in'].data))
        logger.trace 'Parameters', params

        reply = begin
          on_message(name, request['method'].to_sym, params)
        rescue ScriptError, NameError, StandardError, Exception => exc
          logger.error "Exception while calling service: #{name}", exc
          { :exception => {:message => exc.message, :class => exc.class.name} }
        end

        if reply
          logger.debug "Sending Header"
          # For this test we just send back the received header
          client.write(header.to_bson)

          logger.debug "Sending Reply"
          logger.trace 'Reply', reply
          client.write({'out' => BSON::Binary.new(reply.to_bson)}.to_bson)
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

    # Called for each message received from the client
    # Returns a Hash that is sent back to the caller
    def on_message(skynet_name, method, params)
      logger.benchmark_info("Skynet Call: #{skynet_name}##{method}") do
        logger.trace "Method Call: #{method} with parameters:", params
        klass = services[skynet_name]
        raise "Invalid Skynet RPC call, service: #{skynet_name} is not available at this server" unless klass
        # TODO Use pool of services
        service = klass.new
        raise "Invalid Skynet RPC call, method: #{method} does not exist for service: #{skynet_name}" unless service.respond_to?(method)
        service.send(method, params)
      end
    end

    # Returns the supplied camel_cased string as it's class
    def constantize(camel_cased_word)
      names = camel_cased_word.split('::')
      names.shift if names.empty? || names.first.empty?

      constant = Object
      names.each do |name|
        constant = constant.const_defined?(name) ? constant.const_get(name) : constant.const_missing(name)
      end
      constant
    end

  end
end
