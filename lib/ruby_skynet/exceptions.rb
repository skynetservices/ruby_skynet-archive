module RubySkynet
  class Exception < ::RuntimeError; end
  class ProtocolError < Exception; end
  class ServiceException < Exception; end
  class SkynetException < Exception; end
  class ServiceUnavailable < SkynetException; end
end
