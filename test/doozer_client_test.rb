# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet/doozer/client'

# NOTE:
# This test assumes that doozerd is running locally on the default port of 8046

# Unit Test for RubySkynet::Doozer::Client
class DoozerClientTest < Test::Unit::TestCase
  context RubySkynet::Doozer::Client do

    context "without server" do
      should "raise exception when cannot reach doozer server after 5 retries" do
        exception = assert_raise ResilientSocket::ConnectionFailure do
          RubySkynet::Doozer::Client.new(
            # Bad server address to test exception is raised
            :server                 => 'localhost:9999',
            :connect_retry_interval => 0.1,
            :connect_retry_count    => 5)
        end
        assert_match /After 5 connection attempts to host 'localhost:9999': Errno::ECONNREFUSED/, exception.message
      end

    end

    context "with client connection" do
      setup do
        @client = RubySkynet::Doozer::Client.new(:server => 'localhost:8046')
      end

      def teardown
        if @client
          @client.close
          @client.delete('/test/foo')
        end
      end

      should "return current revision" do
        assert @client.current_revision >= 0
      end

      should "successfully set and get data" do
        new_revision = @client.set('/test/foo', 'value')
        result = @client.get('/test/foo')
        assert_equal 'value', result.value
        assert_equal new_revision, result.rev
      end

      should "successfully set and get data using array operators" do
        @client['/test/foo'] = 'value2'
        result = @client['/test/foo']
        assert_equal 'value2', result
      end

      should "fetch directories in a path" do
        @path = '/'
        count = 0
        until @client.directory(@path, count).nil?
          count += 1
        end
        assert count > 0
      end

    end
  end
end