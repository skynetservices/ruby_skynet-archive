require 'semantic_logger'
require 'ruby_skynet/exceptions'
require 'ruby_skynet/version'
require 'ruby_skynet/ruby_skynet'

module RubySkynet
  autoload :Common,          'ruby_skynet/common'
  autoload :Connection,      'ruby_skynet/connection'
  autoload :Client,          'ruby_skynet/client'
  autoload :Service,         'ruby_skynet/service'
  autoload :Server,          'ruby_skynet/server'
  autoload :Zookeeper,       'ruby_skynet/zookeeper'
end

# Autodetect if Zookeeper gem is loaded, otherwise look for Doozer
module RubySkynet
  begin
    require 'zookeeper'
    require 'zookeeper/client'
    require 'ruby_skynet/zookeeper/service_registry'
    # Monkey-patch so that the Zookeeper JRuby code can handle nil values in Zookeeper
    require 'ruby_skynet/zookeeper/extensions/java_base' if defined?(::JRUBY_VERSION)

    # Shortcuts to loaded Registry classes
    ServiceRegistry = RubySkynet::Zookeeper::ServiceRegistry
    CachedRegistry  = RubySkynet::Zookeeper::CachedRegistry
    Registry        = RubySkynet::Zookeeper::Registry
  rescue LoadError
    begin
      require 'ruby_doozer'
      require 'ruby_skynet/doozer/service_registry'

      # Shortcuts to loaded Registry classes
      ServiceRegistry = RubySkynet::Doozer::ServiceRegistry
      CachedRegistry  = Doozer::CachedRegistry
      Registry        = Doozer::Registry
    rescue LoadError
      require 'ruby_skynet/static_service_registry'

      # Use Static Service Registry
      ServiceRegistry = RubySkynet::StaticServiceRegistry
    end

  end
end

if defined?(Rails)
  require 'ruby_skynet/railtie'
end
