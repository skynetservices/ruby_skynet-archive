# Define RubySkynet::Registry based on whether the ZooKeeper or Doozer gem is present
module RubySkynet
  begin
    require 'zookeeper'
    require 'zookeeper/client'
    # Monkey-patch so that the Zookeeper JRuby code can handle nil values in Zookeeper
    require 'ruby_skynet/zookeeper/extensions/java_base' if defined?(::JRUBY_VERSION)
    Registry = RubySkynet::Zookeeper::Registry
  rescue LoadError
    begin
      require 'ruby_doozer'
    rescue LoadError
      raise LoadError, "Must gem install either 'zookeeper' or 'ruby_doozer'. 'zookeeper' is recommended"
    end
    Registry = Doozer::Registry
  end
end