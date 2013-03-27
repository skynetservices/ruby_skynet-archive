# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
$LOAD_PATH.unshift File.dirname(__FILE__)

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet'

# Register an appender if one is not already registered
if SemanticLogger::Logger.appenders.size == 0
  SemanticLogger::Logger.default_level = :debug
  SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('test.log')
end

# Service implementation
class BaseTestService
  include RubySkynet::Service

  # Methods implemented by this service
  # Must take a Hash as input
  # Must Return a Hash response or nil for no response
  def test1(params)
    { 'result' => 'test1' }
  end

  def sleep(params)
    Kernel.sleep params['duration'] || 1
    { 'result' => 'sleep' }
  end

  def fail(params)
    if params['attempt'].to_i >= 2
      { 'result' => 'fail' }
    else
      nil
    end
  end

end

# Service Client
class BaseTestServiceClient
  include RubySkynet::Base

  # Override Name registered in skynet to match server above
  self.skynet_name = 'BaseTestService'
end

# Unit Test
class BaseTest < Test::Unit::TestCase
  context RubySkynet::Base do

    context "with server" do
      setup do
        RubySkynet.region = @region
        RubySkynet::Server.start

        @read_timeout = 3.0
      end

      teardown do
        RubySkynet::Server.stop
      end

      context "with client connection" do
        setup do
          @client = BaseTestServiceClient.new
        end

        should "successfully send and receive data" do
          reply = @client.test1('some' => 'parameters')
          assert_equal 'test1', reply['result']
        end

        should "timeout on receive" do
          request = { 'duration' => @read_timeout + 0.5}

          exception = assert_raise ResilientSocket::ReadTimeout do
            # Read 4 bytes from server
            @client.sleep(request, :read_timeout => @read_timeout)
          end
          assert_match /Timedout after #{@read_timeout} seconds trying to read/, exception.message
        end

      end
    end
  end
end