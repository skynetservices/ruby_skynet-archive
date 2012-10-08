require 'semantic_logger'
require 'resilient_socket'

require 'skynet/exceptions'
require 'skynet/version'
module Skynet
  module Doozer
    autoload :Client, 'skynet/doozer/client'
  end
  autoload :Client, 'skynet/client'
end
