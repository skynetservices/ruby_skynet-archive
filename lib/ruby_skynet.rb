require 'semantic_logger'
require 'ruby_skynet/exceptions'
require 'ruby_skynet/version'
module RubySkynet
  module Doozer
    autoload :Client,   'ruby_skynet/doozer/client'
  end
  autoload :Registry,   'ruby_skynet/registry'
  autoload :Connection, 'ruby_skynet/connection'
  autoload :Common,     'ruby_skynet/common'
  autoload :Client,     'ruby_skynet/client'
  autoload :Service,    'ruby_skynet/service'
  autoload :Server,     'ruby_skynet/server'
end
