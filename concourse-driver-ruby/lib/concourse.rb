require 'thrift'
require 'java-properties'
require_relative 'utils'
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
    def initialize(host: "localhost", port: 1717, username: "admin", password: "admin", environment: "", **kwargs)
        username = username or Utils::Args::find_in_kwargs_by_alias('username', kwargs)
        password = password or Utils::Args::find_in_kwargs_by_alias('password', kwargs)
        prefs = Utils::Args::find_in_kwargs_by_alias('prefs', kwargs)
        if !prefs.nil?
            prefs = File.expand_path(prefs)
            data = JavaProperties.load(prefs)
        else
            data = {}
        end
        @host = data.fetch(:host, host)
        @port = data.fetch(:port, port)
        @username = data.fetch(:username, username)
        @password = data.fetch(:password, password)
        @environment = data.fetch(:environment, environment)
        begin
            @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
            @protocol = Thrift::BinaryProtocol.new(@transport)
            @client = ConcourseService::Client.new(@protocol)
            @transport.open()
        rescue Thrift::Exception
            raise "Could not connect to the Concourse Server at #{@host}:#{@port}"
        end
        @transaction = nil
        authenticate()
    end

    def authenticate()
        begin
            @creds = @client.login(@username, @password, @environment)
        rescue Thrift::Exception => ex
            raise ex
        end
    end

    private :authenticate

end
