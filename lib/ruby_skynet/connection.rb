require 'bson'
require 'gene_pool'
require 'thread_safe'
require 'resilient_socket'
require 'sync_attr'

#
# RubySkynet Client Connection
#
# Handles connecting to Skynet Servers as a host:port pair
#
module RubySkynet
  class Connection
    include SyncAttr
    include SemanticLogger::Loggable

    # Returns the underlying socket being used by a Connection instance
    attr_reader :socket

    # Default Pool configuration
    sync_cattr_accessor :pool_config do
      {
        :pool_size    => 30,   # Maximum number of connections to any one server
        :warn_timeout => 2,    # Log a warning if no connections are available after the :warn_timeout seconds
        :timeout      => 10,   # Raise a Timeout exception if no connections are available after the :timeout seconds
        :idle_timeout => 600,  # Renew a connection if it has been idle for this period of time
      }
    end

    # For each server there is a connection pool keyed on the
    # server address: 'host:port'
    @@connection_pools = ThreadSafe::Hash.new

    # Returns a new RubySkynet connection to the server
    #
    # Parameters:
    #   :read_timeout [Float]
    #     Time in seconds to timeout on read
    #     Can be overridden by supplying a timeout in the read call
    #     Default: 60
    #
    #   :connect_timeout [Float]
    #     Time in seconds to timeout when trying to connect to the server
    #     Default: Half of the :read_timeout ( 30 seconds )
    #
    #   :connect_retry_count [Fixnum]
    #     Number of times to retry connecting when a connection fails
    #     Default: 10
    #
    #   :connect_retry_interval [Float]
    #     Number of seconds between connection retry attempts after the first failed attempt
    #     Default: 0.5
    def initialize(server, params = {})
      self.logger = SemanticLogger["#{self.class.name} [#{server}]"]

      # User configurable options
      params[:read_timeout]           ||= 60
      params[:connect_timeout]        ||= 30
      params[:connect_retry_interval] ||= 0.1
      params[:connect_retry_count]    ||= 5

      # Disable send buffering since it is a RPC call
      params[:buffered] = false

      # For each new connection perform the Skynet handshake
      params[:on_connect] = Proc.new do |socket|
        # Reset user_data on each connection
        socket.user_data = {
          :seq    => 0,
          :logger => logger
        }

        # Receive Service Handshake
        # Registered bool
        #   Registered indicates the state of this service. If it is false, the connection will
        #   close immediately and the client should look elsewhere for this service.
        #
        # ClientID string
        #   ClientID is a UUID that is used by the client to identify itself in RPC requests.
        logger.debug "Waiting for Service Handshake"
        service_handshake = Common.read_bson_document(socket)
        logger.trace 'Service Handshake', service_handshake

        # #TODO When a reconnect returns registered == false need to throw an exception
        # so that this host connection is not used
        registered = service_handshake['registered']
        client_id = service_handshake['clientid']
        socket.user_data[:client_id] = client_id

        # Send blank ClientHandshake
        client_handshake = { 'clientid' => client_id }
        logger.debug "Sending Client Handshake"
        logger.trace 'Client Handshake', client_handshake
        socket.write(client_handshake.to_bson)
      end

      # To prevent strange issues if user incorrectly supplies server names
      params.delete(:servers)
      params[:server] = server

      @socket = ResilientSocket::TCPClient.new(params)
    end

    # Performs a synchronous call to a Skynet server
    #
    # Parameters:
    #   skynet_name [String|Symbol]:
    #     Name of the method to pass in the request
    #   method_name [String|Symbol]:
    #     Name of the method to pass in the request
    #   parameters [Hash]:
    #     Parameters to pass in the request
    #   idempotent [True|False]:
    #     If the request can be applied again to the server without changing its state
    #     Set to true to retry the entire request after the send is successful
    #
    # Returns the Hash result returned from the Skynet Service
    #
    # Raises RubySkynet::ProtocolError
    # Raises RubySkynet::SkynetException
    def rpc_call(request_id, skynet_name, method_name, parameters, idempotent=false)
      logger.benchmark_info "Called #{skynet_name}.#{method_name}" do
        retry_count = 0
        header = nil
        response = nil
        socket.retry_on_connection_failure do |socket|
          header = {
            'servicemethod' => "#{skynet_name}.Forward",
            'seq'           => socket.user_data[:seq]
          }

          logger.debug "Sending Header"
          logger.trace 'Header', header
          socket.write(header.to_bson)

          # The parameters are placed in the request object in BSON serialized form
          request = {
            'clientid'    => socket.user_data[:client_id],
            'in'          => BSON::Binary.new(parameters.to_bson),
            'method'      => method_name.to_s,
            'requestinfo' => {
              'requestid'     => request_id,
              # Increment retry count to indicate that the request may have been tried previously
              'retrycount' => retry_count,
              # TODO: this should be forwarded along in case of services also
              # being a client and calling additional services. If empty it will
              # be stuffed with connecting address
              'originaddress' => ''
            }
          }

          logger.debug "Sending Request"
          logger.trace 'Request', request
          logger.trace 'Parameters:', parameters
          socket.write(request.to_bson)

          # Since Send does not affect state on the server we can also retry reads
          if idempotent
            logger.debug "Reading header from server"
            header = Common.read_bson_document(socket)
            logger.debug 'Response Header', header

            # Read the BSON response document
            logger.debug "Reading response from server"
            response = Common.read_bson_document(socket)
            logger.trace 'Response', response
          end
        end

        # Perform the read outside the retry block since a successful write
        # means that the servers state may have been changed
        unless idempotent
          # Read header first as a separate BSON document
          logger.debug "Reading header from server"
          header = Common.read_bson_document(socket)
          logger.debug 'Response Header', header

          # Read the BSON response document
          logger.debug "Reading response from server"
          response = Common.read_bson_document(socket)
          logger.trace 'Response', response
        end

        # Ensure the sequence number in the response header matches the
        # sequence number sent in the request
        seq_no = header['seq']
        if seq_no != socket.user_data[:seq]
          raise ProtocolError.new("Incorrect Response received, expected seq=#{socket.user_data[:seq]}, received: #{header.inspect}")
        end

        # Increment Sequence number only on successful response
        socket.user_data[:seq] += 1

        # If an error is returned from Skynet raise a Skynet exception
        error = header['error']
        raise SkynetException.new(error) if error.to_s.length > 0

        # If an error is returned from the service raise a Service exception
        error = response['error']
        raise ServiceException.new(error) if error.to_s.length > 0

        # Return Value
        # The return value is inside the response object, it's a byte array of it's own and needs to be deserialized
        result = Hash.from_bson(StringIO.new(response['out'].data))
        logger.trace 'Return Value', result
        result
      end
    end

    # Execute the supplied block with a connection from the pool
    def self.with_connection(server, params={}, &block)
      (@@connection_pools[server] ||= new_connection_pool(server, params)).with_connection(&block)
    end

    def close
      @socket.close if @socket
    end

    ########################
    protected

    # Returns a new connection pool for the specified server
    def self.new_connection_pool(server, params={})
      # Connection pool configuration options
      config = pool_config.dup

      logger = SemanticLogger::Logger.new("#{self.class.name} [#{server}]")

      # Method to call to close idle connections
      config[:close_proc] = :close
      config[:logger]     = logger

      pool = GenePool.new(pool_config) do
        new(server, params)
      end

      # Cleanup corresponding connection pool when a server terminates
      RubySkynet.service_registry.on_server_removed(server) do
        pool = @@connection_pools.delete(server)
        # Cannot close all the connections since they could still be in use
        pool.remove_idle(0) if pool
        #pool.close if pool
        logger.debug "Connection pool released"
      end

      pool
    end

  end

end

