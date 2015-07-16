require 'thrift'
require_relative 'thrift_api/concourse_service'
require_relative 'thrift_api/shared_types'

class Concourse

    # Construct a new instance
    # Params:
    # +host+::
    # +port+::
    # +username+::
    # +password+::
    # +environment+::
    def initialize(host: "localhost", port: 1717, username: "admin", password: "admin", environment: "")
        @host = host
        @port = port
        @username = username
        @password = password
        @environment = environment
        begin
            @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
            @protocol = Thrift::BinaryProtocol.new(@transport)
            @client = ConcourseService::Client.new(@protocol)
            @transport.open()
        rescue Thrift::Exception
            raise "Could not connect to the Concourse Server at #{@host}:#{@port}"
        end
        @transaction = nil
        self.authenticate()
        puts "I'm authenticated"
    end

    def authenticate()
        begin
            @creds = @client.login(@username, @password, @environment)
        rescue Thrift::Exception => ex
            raise ex
        end
    end

end
