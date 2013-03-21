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

# Unit Test
class RegistryTest < Test::Unit::TestCase
  context 'RubySkynet::Service' do

    setup do
      @service_name = 'MyRegistryService'
      @version      = 5
      @region       = 'RegistryTest'
      @hostname     = '127.0.0.1'
      @port         = 2100
      @service_key  = "/services/#{@service_name}/#{@version}/#{@region}/#{@hostname}/#{@port}"
      # Start monitoring doozer for changes
      RubySkynet::Registry.service_registry
    end

    context "without a registered service" do
      should "not be in doozer" do
        RubySkynet::Registry.send(:doozer_pool).with_connection do |doozer|
          assert_equal '', doozer[@service_key]
        end
      end
    end

    context "with a registered service" do
      setup do
        RubySkynet::Registry.register_service(@service_name, @version, @region, @hostname, @port)
      end

      teardown do
        RubySkynet::Registry.deregister_service(@service_name, @version, @region, @hostname, @port)
      end

      should "be in doozer" do
        RubySkynet::Registry.send(:doozer_pool).with_connection do |doozer|
          assert_equal true, doozer[@service_key].length > 20
        end
      end
    end

  end
end