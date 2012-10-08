require 'ruby_skynet/doozer/msg.pb'
require 'semantic_logger'
require 'resilient_socket'
require 'ruby_skynet/doozer/exceptions'
require 'ruby_skynet/doozer/msg.pb'

module RubySkynet
  module Doozer
    class Client

      # Create a resilient client connection to a Doozer server
      def initialize(params={})
        @logger = SemanticLogger::Logger.new(self.class)

        # User configurable options
        params[:read_timeout]           ||= 5
        params[:connect_timeout]        ||= 3
        params[:connect_retry_interval] ||= 0.1
        params[:connect_retry_count]    ||= 3

        # Server name and port where Doozer is running
        # Defaults to 127.0.0.1:8046
        params[:server] ||= '127.0.0.1:8046' unless params[:servers]

        # Disable buffering the send since it is a RPC call
        params[:buffered] = false

        @logger.trace "Socket Connection parameters", params

        # For each new connection
        params[:on_connect] = Proc.new do |socket|
          # Reset user_data on each connection
          socket.user_data = 0
        end

        @socket = ResilientSocket::TCPClient.new(params)
      end

      # Close this client connection to doozer
      def close
        @socket.close if @socket
      end

      # Returns the current Doozer revision
      def current_revision
        invoke(Request.new(:verb => Request::Verb::REV)).rev
      end

      # Set a value in Doozer
      #   path:  Path to the value to be set
      #   value: Value to set
      #   rev:   Revision at which to set the value
      #         If not supplied it will replace the latest version on the server
      #
      # Returns the new revision of the updated value
      #
      # It is recommended to set the revision so that multiple clients do not
      # attempt to update the value at the same time.
      # Setting the revision also allows the call to be retried automatically
      # in the event of a network failure
      def set(path, value, rev=-1)
        invoke(Request.new(:path => path, :value => value, :rev => rev, :verb => Request::Verb::SET), false).rev
      end

      # Sets the current value at the supplied path
      def []=(path,value)
        set(path, value)
      end

      # Return the value at the supplied path and revision
      def get(path, rev = nil)
        invoke(Request.new(:path => path, :rev => rev, :verb => Request::Verb::GET))
      end

      # Returns just the value at the supplied path, not the revision
      def [](path)
        get(path).value
      end

      # Deletes the file at path if rev is greater than or equal to the file's revision.
      # Returns nil when the file was removed
      # Raises an exception if an attempt to remove the file and its revision
      #   is greater than that supplied
      def delete(path, rev=-1)
        invoke(Request.new(:path => path, :rev => rev, :verb => Request::Verb::DEL))
        nil
      end

      # Returns the directory in the supplied path
      # Use offset to get the next
      # returns nil if no further paths are available
      def directory(path, offset = 0, rev = nil)
        begin
          invoke(Request.new(:path => path, :rev => rev, :offset => offset, :verb => Request::Verb::GETDIR))
        rescue RubySkynet::Doozer::ResponseError => exc
          raise exc unless exc.message.include?('RANGE')
          nil
        end
      end

      def stat(path, rev = nil)
        invoke(Request.new(:path => path, :rev => rev, :verb => Request::Verb::STAT))
      end

      def access(secret)
        invoke(Request.new(:path => secret, :verb => Request::Verb::ACCESS))
      end

      # Returns every entry in the supplied path
      # path can also contain wildcard characters such as '*'
      # Example:
      #   hosts = []
      #   walk('/ctl/node/*/addr', current_revision).each do |node|
      #     hosts << node.value unless hosts.include? node.value
      #   end
      def walk(path, rev = nil, offset = 0)
        paths = []
        revision = rev || current_revision
        # Resume walk on network connection failure
        @socket.retry_on_connection_failure do
          while true
            send(Request.new(:path => path, :rev => revision , :offset => offset, :verb => Request::Verb::WALK))
            response = read
            if response.err_code
              break if response.err_code == Response::Err::RANGE
            else
              raise ResponseError.new("#{Response::Err.name_by_value(response.err_code)}: #{response.err_detail}") if response.err_code != 0
            end
            paths << response
            offset += 1
          end
        end
        paths
      end

      # Returns [Array] of hostname [String] with each string
      # representing another Doozer server that can be connected to
      def doozer_hosts
        hosts = []
        walk('/ctl/node/*/addr', current_revision).each do |node|
          hosts << node.value unless hosts.include? node.value
        end
      end

      # TODO Implement watching for changes in a separate thread with it's own
      #      client connection
      #def watch(path, rev = nil)
      #  invoke(Request.new(:path => secret, :verb => Request::Verb::WAIT))
      #end

      #####################
      # protected

      # Call the Doozer server
      #
      # When readonly ==> true the request is always retried on network failure
      # When readonly ==> false the request is retried on network failure
      #   _only_ if a rev has been supplied
      #
      # When modifier is true
      def invoke(request, readonly=true)
        retry_read = readonly || !request.rev.nil?
        response = nil
        @socket.retry_on_connection_failure do
          send(request)
          response = read if retry_read
        end
        # Network error on read must be sent back to caller since we do not
        # know if the modification was made
        response = read unless retry_read
        raise ResponseError.new("#{Response::Err.name_by_value(response.err_code)}: #{response.err_detail}") if response.err_code != 0
        response
      end

      # Send the protobuf Request to Doozer
      def send(request)
        request.tag = 0
        data = request.serialize_to_string
        # An additional header is added to the request indicating the size of the request
        head = [data.length].pack("N")
        @socket.send(head+data)
      end

      # Read the protobuf Response from Doozer
      def read
        # First strip the additional header indicating the size of the subsequent response
        head = @socket.read(4)
        length = head.unpack("N")[0]

        # Since can returns upto 'length' bytes we need to make sure it returns
        # at least 'length' bytes
        # TODO: Make this a binary buffer
        data = ''
        until data.size >= length
          data << @socket.read(length)
        end
        Response.new.parse_from_string(data)
      end

    end
  end
end