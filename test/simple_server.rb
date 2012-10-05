require 'rubygems'
require 'socket'
require 'bson'
require 'semantic_logger'

# This a simple stand-alone server that does not use the Skynet code so that
# the Skynet code can be tested


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

# Simple single threaded server for testing purposes using a local socket
# Sends and receives BSON Messages
class SimpleServer
  attr_reader :thread
  def initialize(port = 2000)
    start(port)
  end

  def start(port)
    @server = TCPServer.open(port)
    @logger = SemanticLogger::Logger.new(self.class)

    @thread = Thread.new do
      loop do
        @logger.debug "Waiting for a client to connect"

        # Wait for a client to connect
        on_request(@server.accept)
      end
    end
  end

  def stop
    if @thread
      @thread.kill
      @thread.join
      @thread = nil
    end
    begin
      @server.close if @server
    rescue IOError
    end
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
  # In a real server each request would be handled in a separate thread
  def on_request(client)
    @logger.debug "Client connected, waiting for data from client"

    # Process handshake
    handshake = {
      'registered' => true,
      'clientid' => '123'
    }
    client.print(BSON.serialize(handshake))
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
        client.print(BSON.serialize(header))

        @logger.debug "Sending Reply"
        @logger.trace 'Reply', reply
        client.print(BSON.serialize({'out' => BSON.serialize(reply).to_s}))
      else
        @logger.debug "Closing client since no reply is being sent back"
        @server.close
        client.close
        @logger.debug "Server closed"
        #@thread.kill
        @logger.debug "thread killed"
        start(2000)
        @logger.debug "Server Restarted"
        break
      end
    end
    # Disconnect from the client
    client.close
    @logger.debug "Disconnected from the client"
  end

end

if $0 == __FILE__
  SemanticLogger::Logger.default_level = :trace
  SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new(STDOUT)
  server = SimpleServer.new(2000)
  server.thread.join
end