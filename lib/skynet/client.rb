#
# Skynet Client
#
# Supports
#   RPC calls to Skynet
#   Skynet Service autodiscovery
#
module Skynet
  class Client

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
    def self.connect(service_name)
      begin
        client = self.new(service_name)
        yield(client)
      ensure
        client.close if client
      end
    end

    def initialize(service_name)
      @service_name = service_name
      @logger = SemanticLogger::Logger.new("#{self.class.name}: #{service_name}")

      @socket = ResilientSocket::TCPClient.new(
        :server                 => 'localhost:9000',
        # Disable buffering the send since it is a RPC call
        :buffered               => false,
        :read_timeout           => 60,
        :connect_timeout        => 30,
        :connect_retry_interval => 0.1,
        :connect_retry_count    => 5,
        # As soon as a connection is established, this block is called
        # to perform any initialization, authentication, and/or handshake
        :on_connect             => Proc.new do
          # TODO
        end
      )
      # Receive Service Handshake
      # Registered bool
      #   Registered indicates the state of this service. If it is false, the connection will
      #   close immediately and the client should look elsewhere for this service.
      #
      # ClientID string
      #   ClientID is a UUID that is used by the client to identify itself in RPC requests.
      @logger.debug "Waiting for Service Handshake"
      service_handshake = self.class.read_bson_document(@socket)
      @logger.trace 'Service Handshake', service_handshake

      # #TODO When a reconnect returns registered == false we need to go back to doozer
      @registered = service_handshake['registered']
      @client_id = service_handshake['clientid'] || BSON::ObjectId.new.to_s

      # Send blank ClientHandshake
      client_handshake = { 'clientid' => @client_id }
      @logger.debug "Sending Client Handshake"
      @logger.trace 'Client Handshake', client_handshake
      @socket.send(BSON.serialize(client_handshake))

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
      # The message consists of 2 distinct BSON messages, the first is the header:
      #   header = {
      #     'servicemethod' =>'ServiceToCall',
      #     'seq'           => 'sequence number of RPC call per TCP session'
      #   }
      #
      # The second part is the BSON serialized object to pass to the above service
      #
      @logger.benchmark_info "Called Skynet Service: #{@service_name}.#{method_name}" do

        # Resilient Send
        @socket.retry_on_connection_failure do |socket|
          # user_data is maintained per session and a different session could
          # be supplied with each retry
          socket.user_data ||= 0
          header = {
#            'servicemethod' => "#{@service_name}.#{method_name}",
            'servicemethod' => method_name.to_s,
            'seq'           => socket.user_data,
            'clientid'      => @client_id
          }
          @logger.debug "Sending Header"
          @logger.trace 'Header', header
          socket.send(BSON.serialize(header))

          @logger.debug "Sending Body"
          @logger.trace 'Body', parameters
          socket.send(BSON.serialize(parameters))
        end

        # Once send is successful it could have been processed, so we can no
        # longer retry now otherwise we could create a duplicate

        # #<BSON::OrderedHash:0x97ac8 {"servicemethod"=>"CSPIDFService.Process", "seq"=>414, "error"=>""}>
        @logger.debug "Reading header from server"
        header = self.class.read_bson_document(@socket)
        @logger.debug 'Header', header

        @logger.debug "Reading body from server"
        response = self.class.read_bson_document(@socket)
        @logger.trace 'Response', response

        # Ensure the sequence number in the response header matches the
        # sequence number sent in the request
        if seq_no = header['seq']
          raise ProtocolError.new("Incorrect Response received, expected seq=#{@socket.user_data}, received: #{header.inspect}") if seq_no != @socket.user_data
        else
          raise ProtocolError.new("Invalid Response header, missing 'seq': #{header.inspect}")
        end

        # If an error is returned convert it to a ServerError exception
        if error = header['error']
          raise SkynetException.new(error) if error.to_s.length > 0
        end

        # Increment Sequence number only on successful response
        @socket.user_data += 1
        response
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

  end
end