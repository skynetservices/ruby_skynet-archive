# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet'

# Register an appender if one is not already registered
if SemanticLogger::Logger.appenders.size == 0
  SemanticLogger::Logger.default_level = :trace
  SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('test.log')
end

# Unit Test for ResilientSocket::TCPClient
class RubySkynetServiceTest < Test::Unit::TestCase
  context 'RubySkynet::Service' do
    context "with server" do
      setup do
        RubySkynet::Server.port = 2100
        RubySkynet::Server.region = 'Test'
        RubySkynet::Server.hostname = 'localhost'
        @server = RubySkynet::Server.new
        @service_name = 'RubySkynet.Service'
        @version = 1
        @region = 'Test'
        @doozer_key = "/services/#{@service_name}/#{@version}/#{@region}/localhost/2100"

        # Register Service
        RubySkynet::Service
      end

      teardown do
        begin
          @server.terminate if @server
        rescue Celluloid::DeadActorError
        end
      end

      should "have correct service key" do
        assert_equal @doozer_key, RubySkynet::Service.service_key
      end

      should "register service" do
        RubySkynet::Registry.doozer_pool.with_connection do |doozer|
          assert_equal true, doozer[@doozer_key].length > 20
        end
      end

      context "calling with a client" do
        setup do
          @client = RubySkynet::Client.new(@service_name, @version, @region)
        end

        should "successfully send and receive data" do
          reply = @client.call(:echo, 'some' => 'parameters')
          assert_equal 'test1', reply #['result']
        end

#        should "timeout on receive" do
#          request = { 'duration' => @read_timeout + 0.5}
#
#          exception = assert_raise ResilientSocket::ReadTimeout do
#            # Read 4 bytes from server
#            @client.call('sleep', request, :read_timeout => @read_timeout)
#          end
#          assert_match /Timedout after #{@read_timeout} seconds trying to read/, exception.message
#        end
      end

    end
  end
end