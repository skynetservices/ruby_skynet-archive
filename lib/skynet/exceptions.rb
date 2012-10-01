module Skynet
  class Exception < ::RuntimeError; end
  class ProtocolError < Exception; end
  class SkynetException < Exception; end
end
