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
        records ||= kwargs.fetch(:record, nil)
        records ||= kwargs.fetch(:records, nil)
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

    def get(*args, **kwargs)
        keys, criteria, records, timestamp = args
        criteria ||= Utils::Args::find_in_kwargs_by_alias('criteria', kwargs)
        keys ||= kwargs.fetch(:key, nil)
        records = records ||= kwargs.fetch(:record, nil)
        timestamp ||= Utils::Args::find_in_kwargs_by_alias('timestamp', kwargs)
        timestr = timestamp.is_a? String
        # Try to figure out intent if args were used instead of kwargs
        if criteria.is_a? Integer
            records = criteria
            criteria = nil
        end
        if records.is_a? Array and keys.nil? and timestamp.nil?
            data = @client.getRecords records, @creds, @transaction, @environment
        elsif records.is_a? Array and !timestamp.nil? and !timestr and keys.nil?
            data = @client.getRecordsTime records, timestamp, @creds, @transaction, @environment
        elsif records.is_a? Array and !timestamp.nil? and timestr and keys.nil?
            data = @client.getRecordsTimestr records, timestamp, @creds, @transaction, @environment
        elsif records.is_a? Array and keys.is_a? Array and timestamp.nil?
            data = @client.getKeysRecords keys, records, @creds, @transaction, @environment
        elsif records.is_a? Array and keys.is_a? Array and !timestamp.nil? and !timestr
            data = @client.getKeysRecordsTime keys, records, timestamp, @creds, @transaction, @environment
        elsif records.is_a? Array and keys.is_a? Array and !timestamp.nil? and timestr
            data = @client.getKeysRecordsTimestr keys, records, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? Array and !criteria.nil? and timestamp.nil?
            data = @client.getKeysCcl keys, criteria, @creds, @transaction, @environment
        elsif keys.is_a? Array and !criteria.nil? and !timestamp.nil? and !timestr
            data = @client.getKeysCclTime keys, criteria, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? Array and !criteria.nil? and !timestamp.nil? and timestr
            data = @client.getKeysCclTimestr keys, criteria, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? Array and records.is_a? Integer and timestamp.nil?
            data = @client.getKeysRecord keys, records, @creds, @transaction, @environment
        elsif keys.is_a? Array and records.is_a? Integer and !timestamp.nil? and !timestr
            data = @client.getKeysRecordTime keys, records, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? Array and records.is_a? Integer and timestamp.nil? and timestr
            data = @client.getKeysRecordTimestr keys, records, timestamp, @creds, @transaction, @environment
        elsif !criteria.nil? and keys.nil? and timestamp.nil?
            data = @client.getCcl criteria, @creds, @transaction, @environment
        elsif !criteria.nil? and !timestamp.nil? and !timestr and keys.nil?
            data = @client.getCclTime criteria, timestamp, @creds, @transaction, @environment
        elsif !criteria.nil? and !timestamp.nil? and timestr and keys.nil?
            data = @client.getCclTimestr criteria, timestamp, @creds, @transaction, @environment
        elsif records.is_a? Integer and keys.nil? and timestamp.nil?
            data = @client.getRecord records, @creds, @transaction, @environment
        elsif records.is_a? Integer and !timestamp.nil? and !timestr and keys.nil?
            data = @client.getRecordTime records, timestamp, @creds, @transaction, @environment
        elsif records.is_a? Integer and !timestamp.nil? and timestr and keys.nil?
            data = @client.getRecordTimestr records, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? String and !criteria.nil? and timestamp.nil?
            data = @client.getKeyCcl keys, criteria, @creds, @transaction, @environment
        elsif keys.is_a? String and !criteria.nil? and !timestamp.nil and !timestr
            data = @client.getKeyCclTime keys, criteria, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? String and !criteria.nil? and !timestamp.nil? and timestr
            data = @client.getKeyCclTimestr keys, criteria, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? String and records.is_a? Array and timestamp.nil?
            data = @client.getKeyRecords keys, records, @creds, @transaction, @environment
        elsif keys.is_a? String and records.is_a? Integer and timestamp.nil?
            data = @client.getKeyRecord keys, records, @creds, @transaction, @environment
        elsif keys.is_a? String and records.is_a? Array and !timestamp.nil? and !timestr
            data = @client.getKeyRecordsTime keys, records, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? String and records.is_a? Array and !timestamp.nil? and timestr
            data = @client.getKeyRecordsTimestr keys, records, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? String and records.is_a? Integer and !timestamp.nil? and !timestr
            data = @client.getKeyRecordTime keys, records, timestamp, @creds, @transaction, @environment
        elsif keys.is_a? String and records.is_a? Integer and !timestamp.nil and timestr
            data = @client.getKeyRecordTimestr keys, records, timestamp, @creds, @transaction, @environment
        else
            Utils::Args::require('criteria or (key and record)')
        end
        return Utils::Convert::rubyify data
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
