require 'semantic_logger'
require 'resilient_socket'

require 'ruby_skynet/exceptions'
require 'ruby_skynet/version'
module RubySkynet
  module Doozer
    autoload :Client, 'ruby_skynet/doozer/client'
  end
  autoload :Registry,   'ruby_skynet/registry'
  autoload :Connection, 'ruby_skynet/connection'
  autoload :Client,     'ruby_skynet/client'
end
