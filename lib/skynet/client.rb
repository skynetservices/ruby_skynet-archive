require 'bson'
require 'sync_attr'
require 'multi_json'

#
# Skynet Client
#
# Supports
#   RPC calls to Skynet
#   Skynet Service autodiscovery
#
module Skynet
  class Client
    include SyncAttr

    # Default doozer configuration
    # To replace this default, set the config as follows:
    #    Skynet::Client.doozer_config = { .... }
    sync_attr_accessor :doozer_config do
      {
        :server                 => '127.0.0.1:8046',
        :read_timeout           => 5,
        :connect_timeout        => 3,
        :connect_retry_interval => 0.1,
        :connect_retry_count    => 3
      }
    end

    # Lazy initialize Doozer Client
    sync_cattr_reader :doozer do
      Doozer::Client.new
    end

    # Create a client connection, call the supplied block and close the connection on
    # completion of the block
    #
    # Example
    #
    #  require 'skynet'
    #  SemanticLogger.default_level = :trace
    #  SemanticLogger.appenders << SemanticLogger::Appender::File(STDOUT)
    #  Skynet::Client.connect('TutorialService') do |tutorial_service|
    #    p tutorial_service.call(:value => 5)
    #  end
    def self.connect(service_name, params={})
      begin
        client = self.new(service_name, params)
        yield(client)
      ensure
        client.close if client
      end
    end

    # Returns a new Skynet Client for the named service
    #
    # Parameters:
    #   :service_name
    #     Name of the service to look for and connect to on Skynet
    #
    #   :doozer_servers [Array of String]
    #     Array of URL's of doozer servers to connect to with port numbers
    #     ['server1:2000', 'server2:2000']
    #
    #     The second server will only be attempted once the first server
    #     cannot be connected to or has timed out on connect
    #     A read failure or timeout will not result in switching to the second
    #     server, only a connection failure or during an automatic reconnect
    #
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
    def initialize(service_name, params = {})
      @service_name = service_name
      @logger = SemanticLogger::Logger.new("#{self.class.name}: #{service_name}")

      # User configurable options
      params[:read_timeout]           ||= 60
      params[:connect_timeout]        ||= 30
      params[:connect_retry_interval] ||= 0.1
      params[:connect_retry_count]    ||= 5

      # If Server name and port of where Skynet Service is running
      # is not supplied look for it in Doozer
      unless params[:server] || params[:servers]
        params[:server] = self.class.server_for(service_name)
      end

      # Disable buffering the send since it is a RPC call
      params[:buffered] = false

      @logger.trace "Socket Connection parameters", params

      # For each new connection perform the Skynet handshake
      params[:on_connect] = Proc.new do |socket|
        # Reset user_data on each connection
        socket.user_data = 0

        # Receive Service Handshake
        # Registered bool
        #   Registered indicates the state of this service. If it is false, the connection will
        #   close immediately and the client should look elsewhere for this service.
        #
        # ClientID string
        #   ClientID is a UUID that is used by the client to identify itself in RPC requests.
        @logger.debug "Waiting for Service Handshake"
        service_handshake = self.class.read_bson_document(socket)
        @logger.trace 'Service Handshake', service_handshake

        # #TODO When a reconnect returns registered == false we need to go back to doozer
        @registered = service_handshake['registered']
        @client_id = service_handshake['clientid']

        # Send blank ClientHandshake
        client_handshake = { 'clientid' => @client_id }
        @logger.debug "Sending Client Handshake"
        @logger.trace 'Client Handshake', client_handshake
        socket.send(BSON.serialize(client_handshake))
      end

      @socket = ResilientSocket::TCPClient.new(params)
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
    # Raises Skynet::ProtocolError
    # Raises Skynet::SkynetException
    def call(method_name, parameters)
      # Skynet requires BSON RPC Calls to have the following format:
      # https://github.com/bketelsen/skynet/blob/protocol/protocol.md
      request_id = BSON::ObjectId.new.to_s
      @logger.tagged request_id do
        @logger.benchmark_info "Called Skynet Service: #{@service_name}.#{method_name}" do

          # Resilient Send
          retry_count = 0
          @socket.retry_on_connection_failure do |socket|
            # user_data is maintained per session and a different session could
            # be supplied with each retry
            socket.user_data ||= 0
            header = {
              'servicemethod' => "#{@service_name}.Forward",
              'seq'           => socket.user_data,
            }
            @logger.debug "Sending Header"
            @logger.trace 'Header', header
            socket.send(BSON.serialize(header))

            @logger.trace 'Parameters:', parameters

            # The parameters are placed in the request object in BSON serialized
            # form
            request = {
              'clientid'    => @client_id,
              'in'          => BSON.serialize(parameters).to_s,
              'method'      => method_name.to_s,
              'requestinfo' => {
                'requestid'     => request_id,
                # Increment retry count to indicate that the request may have been tried previously
                # TODO: this should be incremented if request is retried,
                'retrycount'    => retry_count,
                # TODO: this should be forwarded along in case of services also
                # being a client and calling additional services. If empty it will
                # be stuffed with connecting address
                'originaddress' => ''
              }
            }

            @logger.debug "Sending Request"
            @logger.trace 'Request', request
            socket.send(BSON.serialize(request))
          end

          # Once send is successful it could have been processed, so we can no
          # longer retry now otherwise we could create a duplicate
          # retry_count += 1

          # Read header first as a separate BSON document
          @logger.debug "Reading header from server"
          header = self.class.read_bson_document(@socket)
          @logger.debug 'Header', header

          # Read the BSON response document
          @logger.debug "Reading response from server"
          response = self.class.read_bson_document(@socket)
          @logger.trace 'Response', response

          # Ensure the sequence number in the response header matches the
          # sequence number sent in the request
          if seq_no = header['seq']
            raise ProtocolError.new("Incorrect Response received, expected seq=#{@socket.user_data}, received: #{header.inspect}") if seq_no != @socket.user_data
          else
            raise ProtocolError.new("Invalid Response header, missing 'seq': #{header.inspect}")
          end

          # Increment Sequence number only on successful response
          @socket.user_data += 1

          # If an error is returned from Skynet raise a Skynet exception
          if error = header['error']
            raise SkynetException.new(error) if error.to_s.length > 0
          end

          # If an error is returned from the service raise a Service exception
          if error = response['error']
            raise ServiceException.new(error) if error.to_s.length > 0
          end

          # Return Value
          # The return value is inside the response object, it's a byte array of it's own and needs to be deserialized
          result = BSON.deserialize(response['out'])
          @logger.trace 'Return Value', result
          result
        end
      end
    end

    # Returns a BSON document read from the socket.
    # Returns nil if the operation times out or if a network
    #         connection failure occurs
    def self.read_bson_document(socket)
      bytebuf = BSON::ByteBuffer.new
      # Read 4 byte size of following BSON document
      bytes = ''
      socket.read(4, bytes)

      # Read BSON document
      sz = bytes.unpack("V")[0]
      raise "Invalid Data received from server:#{bytes.inspect}" unless sz

      bytebuf.append!(bytes)
      bytes = ''
      sz -= 4
      until bytes.size >= sz
        buf = ''
        socket.read(sz, buf)
        bytes << buf
      end
      bytebuf.append!(bytes)
      return BSON.deserialize(bytebuf)
    end

    def close()
      @socket.close
    end

    ##############################
    #protected

    # Returns [Array] of the hostname and port pair [String] that implements a particular service
    # Performs a doozer lookup to find the servers
    #
    #   service_name:
    #   version: Version of service to locate
    #            Default: Find latest version
    def self.registered_implementers(service_name, version = '*', region = 'Development')
      hosts = []
      doozer.walk("/services/#{service_name}/#{version}/#{region}/*/*").each do |node|
        entry = MultiJson.load(node.value)
        hosts << entry if entry['Registered']
      end
      hosts
    end

    # Randomly returns a server that implements the requested service
    def self.server_for(service_name, version = '*', region = 'Development')
      hosts = registered_implementers(service_name, version, region)
      service = hosts[rand(hosts.size)]['Config']['ServiceAddr']
      "#{service['IPAddress']}:#{service['Port']}"
    end

  end
end
