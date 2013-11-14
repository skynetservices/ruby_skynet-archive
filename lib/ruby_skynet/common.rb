require 'stringio'
require 'socket'
require 'semantic_logger'
require 'bson'

module RubySkynet
  module Common

    BINARY_ENCODING = Encoding.find("binary")

    # Returns a BSON document read from the socket.
    # Returns nil if the operation times out or if a network
    #         connection failure occurs
    def self.read_bson_document(socket)
      # Read 4 byte size of following BSON document
      if bytes = socket.read(4)
        bytes.force_encoding(BINARY_ENCODING)
        # Read BSON document
        sz = bytes.unpack("V")[0]
        raise "Invalid Data received from server:#{bytes.inspect}" unless sz

        bytes << socket.read(sz - 4)
        raise "Socket is not returning #{sz} requested bytes. #{bytes.length} bytes returned" unless sz == bytes.length
        Hash.from_bson(StringIO.new(bytes))
      end
    end

    # Returns the local ip address being used by this machine to talk to the
    # internet. By default connects to Google and determines what IP Address is used locally
    def self.local_ip_address(remote_ip = 'google.com')
      @@local_ip_address ||= ::UDPSocket.open {|s| s.connect(remote_ip, 1); s.addr.last }
    end

    def self.included(base)
      base.extend ClassMethods
      base.class_eval do
        include SemanticLogger::Loggable
      end
    end

    module ClassMethods
      # Name of this service to Register with Skynet
      # Default: class name
      def skynet_name
        @skynet_name ||= name.gsub('::', '.')
      end

      def skynet_name=(skynet_name)
        @skynet_name = skynet_name
      end

      # Version of this service to register with Skynet
      # Default: nil
      def skynet_version
        @skynet_version ||= nil
      end

      def skynet_version=(skynet_version)
        @skynet_version = skynet_version
      end

      # Region within which this service is defined
      # Default: RubySkynet.region
      def skynet_region
        @skynet_region || ::RubySkynet.region
      end

      def skynet_region=(skynet_region)
        @skynet_region = skynet_region
      end

    end

  end
end