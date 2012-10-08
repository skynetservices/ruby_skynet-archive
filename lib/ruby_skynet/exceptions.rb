module RubySkynet
  class Exception < ::RuntimeError; end
  class ProtocolError < Exception; end
  class SkynetException < Exception; end
  class ServiceException < Exception; end
end
