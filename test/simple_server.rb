require 'rubygems'
require 'socket'
require 'bson'
require 'semantic_logger'
require 'celluloid/io'

# This a simple stand-alone server that does not use the Skynet code so that
# the Skynet code can be tested

# Simple single threaded server for testing purposes using a local socket
# Sends and receives BSON Messages
class SimpleServer
  include Celluloid::IO

  def initialize(port)
    # Since we included Celluloid::IO, we're actually making a
    # Celluloid::IO::TCPServer here
    @server = TCPServer.new('127.0.0.1', port)
    @logger = SemanticLogger::Logger.new(self.class)
    async.run
  end

  def run
    loop do
      @logger.debug "Waiting for a client to connect"
      async.handle_connection(@server.accept)
    end
  end

  def finalize
    @server.close if @server
  end

  # Called for each message received from the client
  # Returns a Hash that is sent back to the caller
  def on_message(method, params)
    case method
    when 'test1'
      { 'result' => 'test1' }
    when 'sleep'
      sleep params['duration'] || 1
      { 'result' => 'sleep' }
    when 'fail'
      if params['attempt'].to_i >= 2
        { 'result' => 'fail' }
      else
        nil
      end
    else
      { 'result' => "Unknown method: #{method}" }
    end
  end

  # Called for each client connection
  def handle_connection(client)
    @logger.debug "Client connected, waiting for data from client"

    # Process handshake
    handshake = {
      'registered' => true,
      'clientid' => '123'
    }
    client.write(BSON.serialize(handshake).to_s)
    read_bson_document(client)

    while(header = read_bson_document(client)) do
      @logger.debug "\n******************"
      @logger.debug "Received Request"
      @logger.trace 'Header', header

      request = read_bson_document(client)
      @logger.trace 'Request', request
      break unless request

      if reply = on_message(request['method'], BSON.deserialize(request['in']))
        @logger.debug "Sending Header"
        # For this test we just send back the received header
        client.write(BSON.serialize(header).to_s)

        @logger.debug "Sending Reply"
        @logger.trace 'Reply', reply
        client.write(BSON.serialize({'out' => BSON.serialize(reply).to_s}).to_s)
      else
        @logger.debug "Closing client since no reply is being sent back"
        @server.close
        client.close
        @logger.debug "Server closed"
        async.run
        @logger.debug "Server Restarted"
        break
      end
    end
    # Disconnect from the client
    client.close
    @logger.debug "Disconnected from the client"
  end

  # Read the bson document, returning nil if the IO is closed
  # before receiving any data or a complete BSON document
  def read_bson_document(io)
    bytebuf = BSON::ByteBuffer.new
    # Read 4 byte size of following BSON document
    bytes = io.read(4)
    return unless bytes
    # Read BSON document
    sz = bytes.unpack("V")[0]
    bytebuf.append!(bytes)
    bytes = io.read(sz-4)
    return unless bytes
    bytebuf.append!(bytes)
    return BSON.deserialize(bytebuf)
  end

end

if $0 == __FILE__
  SemanticLogger::Logger.default_level = :trace
  SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new(STDOUT)
  Celluloid.logger = SemanticLogger::Logger.new('Celluloid')
  server = SimpleServer.new(2000)
  server.thread.join
end