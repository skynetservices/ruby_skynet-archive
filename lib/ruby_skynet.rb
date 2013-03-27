require 'semantic_logger'
require 'ruby_skynet/exceptions'
require 'ruby_skynet/version'
require 'ruby_skynet/ruby_skynet'
module RubySkynet
  module Doozer
    autoload :Client,   'ruby_skynet/doozer/client'
  end
  autoload :Registry,   'ruby_skynet/registry'
  autoload :Connection, 'ruby_skynet/connection'
  autoload :Base,       'ruby_skynet/base'
  autoload :Common,     'ruby_skynet/common'
  autoload :Client,     'ruby_skynet/client'
  autoload :Service,    'ruby_skynet/service'
  autoload :Server,     'ruby_skynet/server'
end

if defined?(Rails)
  require 'ruby_skynet/railtie'
end
