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

    def abort
        if !@transaction.nil?
            token = @transaction
            @transaction = nil
            @client.abort @creds, token, @environment
        end
    end

    def add(*args, **kwargs)
        key, value, records = args
        key = kwargs.fetch(:key, key)
        value = kwargs.fetch(:value, value)
        records = kwargs.fetch(:record, nil) or kwargs.fetch(:records, nil) or records
        value = Utils::Convert::ruby_to_thrift value unless value.nil?
        if records.nil? and key and value
            return @client.addKeyValue key, value, @creds, @transaction, @environment
        elsif records.is_a? Array and key and value
            return @client.addKeyValueRecords key, value, records, @creds, @transaction, @environment
        elsif records.is_a? Integer and key and value
            return @client.addKeyValueRecord key, value, records, @creds, @transaction, @environment
        else
            Utils::Args::require 'key and value'
        end
    end

    def get(keys=nil, criteria=nil, records=nil, timestamp=nil, **kwargs)
        criteria = criteria or Utils::Args::find_in_kwargs_by_alias('criteria', kwargs)
        keys = keys or kwargs.fetch('key', nil)
        records = records or kwargs.fetch('record', nil)
        timestamp = timestamp or Utils::Args::find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = timestamp.is_a? String
        data = @client.getKeyRecord keys, records, @creds, @transaction, @environment
        data = Utils::Convert::thrift_to_ruby data
        return data
    end

    def logout()
        @client.logout(@creds, @environment)
    end

    def stage
        @transaction = @client.stage @creds, @environment
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
