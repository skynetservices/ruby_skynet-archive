require 'semantic_logger'
require 'ruby_skynet/exceptions'
require 'ruby_skynet/version'
require 'ruby_skynet/ruby_skynet'
require 'ruby_skynet/zookeeper'

module RubySkynet
  autoload :Base,            'ruby_skynet/base'
  autoload :Common,          'ruby_skynet/common'
  autoload :Connection,      'ruby_skynet/connection'
  autoload :Client,          'ruby_skynet/client'
  autoload :Service,         'ruby_skynet/service'
  autoload :Server,          'ruby_skynet/server'
  autoload :ServiceRegistry, 'ruby_skynet/service_registry'
end

if defined?(Rails)
  require 'ruby_skynet/railtie'
end
