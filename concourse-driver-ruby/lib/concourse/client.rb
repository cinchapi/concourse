# Copyright (c) 2015 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'concourse/thrift/concourse_service'
require 'concourse/thrift/shared_types'
require 'json'

module Concourse

    # This is an alias for the Concourse::Client constructor
    # @return [Concourse::Client] the handle
    def self.connect(host: "localhost", port: 1717, username: "admin", password: "admin", environment: "", **kwargs)
        return Concourse::Client.new(host: host, port: port, username: username, password: password, environment: environment, **kwargs)
    end

    # Concourse is a self-tuning database that makes it easier to quickly build
    # reliable and scalable systems. Concourse dynamically adapts to any
    # application and offers features like automatic indexing, version control,
    # and distributed ACID transactions within a smart data platform that
    # manages itself, reduces costs and allows developers to focus on what
    # really matters.
    #
    # == Data Model
    # The Concourse data model is lightweight and flexible. Unlike other
    # databases, Concourse is completely schemaless and does not hold data in
    # tables or collections. Concourse is simply a distributed document-graph
    # where data is stored in records (similar to documents or rows in other
    # databases). Each record has multiple keys. And each key has one or more
    # distinct values. Like any graph, you can link records to one another. And
    # the structure of one record does not affect the structure of another.
    #
    # *Record*: A logical grouping of data about a single person, place or thing
    # (i.e. an object). Each record is identified by a unique primary key.
    # *Key*: A attribute that maps to one or more distinct values.
    # *Value*:  A dynamically typed quantity.
    #
    # == Data Types
    # Concourse natively stores the following primitives: boolean, double,
    # integer, string (UTF-8) and Tag (a string that is not full text
    # searchable). Any other data type will be stored as its to_s
    # representation.
    #
    # == Links
    # Concourse allows linking a key in one record to another record using the
    # link() function. Links are retrievable and queryable just like any other
    # value.
    #
    # == Transactions
    # By default, Concourse conducts every operation in autocommit mode where
    # every change is immediately written. You can also stage a group of
    # operations in an ACID transaction. Transactions are managed using the
    # #stage, #commit and #abort commands.
    #
    # == Version Control
    # Concourse automatically tracks every changes to data and the API exposes
    # several methods to tap into this feature.
    # 1) You can get() and select() previous version of data by specifying a
    # timestamp using natural language or a unix timestamp integer in
    # microseconds. 2) You can browse() and find() records that matched a
    # criteria in the past by specifying a timestamp using natural language or a
    # unix timestamp integer in microseconds. 3) You can audit() and diff()
    # changes over time, revert() to previous states and chronologize() how data
    # has evolved over a range of time.
    #
    # @author Jeff Nelson
    class Client

        # Initialize a new client connection
        # @param host [String] the server host
        # @param port [Integer] the listener port
        # @param username [String] the username with which to connect
        # @param password [String] the password for the username
        # @param environment [String] the environment to use, by default the default_environment` in the server's concourse.prefs file is used
        # @option kwargs [String] :prefs  You may specify the path to a preferences file using the 'prefs' keyword argument. If a prefs file is supplied, the values contained therewithin for any of the arguments above become the default if those arguments are not explicitly given values.
        #
        # @return [Client] The handle
        def initialize(host = "localhost", port = 1717, username = "admin", password = "admin", environment = "", **kwargs)
            host = kwargs.fetch(:host, host)
            port = kwargs.fetch(:port, port)
            username = kwargs.fetch(:username, username)
            password = kwargs.fetch(:password, password)
            environment = kwargs.fetch(:environment, environment)
            username = username or ::Utils::Args::find_in_kwargs_by_alias('username', kwargs)
            password = password or Utils::Args::find_in_kwargs_by_alias('password', kwargs)
            prefs = Utils::Args::find_in_kwargs_by_alias('prefs', kwargs)
            if !prefs.nil?
                prefs = File.expand_path(prefs)
                data = ::JavaProperties.load(prefs)
            else
                data = {}
            end
            @host = data.fetch(:host, host)
            @port = data.fetch(:port, port)
            @username = data.fetch(:username, username)
            @password = data.fetch(:password, password)
            @environment = data.fetch(:environment, environment)
            begin
                @transport = ::Thrift::BufferedTransport.new(::Thrift::Socket.new(@host, @port))
                @protocol = ::Thrift::BinaryProtocol.new(@transport)
                @client = Thrift::ConcourseService::Client.new(@protocol)
                @transport.open()
            rescue ::Thrift::Exception
                raise "Could not connect to the Concourse Server at #{@host}:#{@port}"
            end
            @transaction = nil
            authenticate()
        end

        # Abort the current transaction and discard any changes that were
        # staged. After returning, the driver will return to autocommit mode and
        # all subsequent changes will be committed imediately.
        # @return [Void]
        def abort
            if !@transaction.nil?
                token = @transaction
                @transaction = nil
                @client.abort @creds, token, @environment
            end
        end

        # Add a value if it does not already exist.
        # @return [Boolean, Hash, Integer]
        # @overload add(key, value, record)
        #   Add a value to a field in a single record.
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @param [Integer] record The record where the data is added
        #   @return [Boolean] A flag that indicates whether the value was added to the field
        # @overload add(key, value, records)
        #   Add a value to a field in multiple records.
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @param [Array] records The records where the data is added
        #   @return [Hash] A mapping from each record to a Boolean flag that indicates whether the value was added to the field
        # @overload add(key, value)
        #   Add a value to a field in a new record.
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @return [Integer] The id of the new record where the data was added
        def add(*args, **kwargs)
            key, value, records = args
            key ||= kwargs.fetch(:key, key)
            value ||= kwargs.fetch(:value, value)
            records ||= kwargs.fetch(:record, nil)
            records ||= kwargs.fetch(:records, nil)
            value = value.to_thrift unless value.nil?
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

        # Describe changes made to a record or a field over time.
        # @return [Hash]
        # @overload audit(key, record)
        #   Describe all changes made to a field over time.
        #   @param [String] key the field name
        #   @param [Integer] record the record that contains the field
        #   @return [Hash] A mapping from timestamp to a description of the change that occurred
        # @overload audit(key, record, start)
        #   Describe changes made to a field since the specified _start_ timestamp.
        #   @param [String] key the field name
        #   @param [Integer] record the record that contains the field
        #   @param [Integer, String] start The earliest timestamp to check
        #   @return [Hash] A mapping from timestamp to a description of the change that occurred
        # @overload audit(key, record, start, end)
        #   Describe changes made to a field between the specified _start_ timestamp and the _end_ timestamp.
        #   @param [String] key the field name
        #   @param [Integer] record the record that contains the field
        #   @param [Integer, String] start The earliest timestamp to check
        #   @param [Integer, String] end The latest timestamp to check
        #   @return [Hash] A mapping from timestamp to a description of the change that occurred
        # @overload audit(record)
        #   Describe all changes made to a record over time.
        #   @param [Integer] record The record to audit
        #   @return [Hash] A mapping from timestamp to a description of the change that occurred
        # @overload audit(record, start)
        #   Describe changes made to a record since the specified _start_ timestamp.
        #   @param [Integer] record The record to audit
        #   @param [Integer, String] start The earlist timestamp to check
        #   @return [Hash] A mapping from timestamp to a description of the change that occurred
        # @overload audit(record, start, end)
        #   Describe changes made to a record between the specified _start_ timestamp and the _end_ timestamp
        #   @param [Integer] record The record to audit
        #   @param [Integer, String] start The earlist timestamp to check
        #   @param [Integer, String] end The latest timestamp to check
        #   @return [Hash] A mapping from timestamp to a description of the change that occurred
        def audit(*args, **kwargs)
            key, record, start, tend = args
            key ||= kwargs[:key]
            record ||= kwargs[:record]
            start ||= kwargs[:start]
            start ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            tend ||= kwargs[:end]
            startstr = start.is_a? String
            endstr = tend.is_a? String

            # If the first arg is an Integer, then assume that we are auditing
            # an entire record.
            if key.is_a? Integer
                record = key
                key = nil
            end

            if key and record and start and !startstr and tend and !endstr
                data = @client.auditKeyRecordStartEnd key, record, start, tend, @creds, @transaction, @environment
            elsif key and record and start and startstr and tend and endstr
                data = @client.auditKeyRecordStartstrEndstr key, record, start, tend, @creds, @transaction, @environment
            elsif key and record and start and !startstr
                data = @client.auditKeyRecordStart key, record, start, @creds, @transaction, @environment
            elsif key and record and start and startstr
                data = @client.auditKeyRecordStartstr key, record, start, @creds, @transaction, @environment
            elsif key and record
                data = @client.auditKeyRecord key, record, @creds, @transaction, @environment
            elsif record and start and !startstr and tend and !endstr
                data = @client.auditRecordStartEnd record, start, tend, @creds, @transaction, @environment
            elsif record and start and startstr and tend and endstr
                data = @client.auditRecordStartstrEndstr record, start, tend, @creds, @transaction, @environment
            elsif record and start and !startstr
                data = @client.auditRecordStart record, start, @creds, @transaction, @environment
            elsif record and start and startstr
                data = @client.auditRecordStartstr record, start, @creds, @transaction, @environment
            elsif record
                data = @client.auditRecord record, @creds, @transaction, @environment
            else
                Utils::Args::require 'record'
            end
            return data
        end

        # View the values that have been indexed.
        # @return [Hash]
        # @overload browse(key)
        #   View that values that are indexed for _key_.
        #   @param [String] key The field name
        #   @return [Hash] A Hash mapping each indexed value to an Array of records where the value is contained
        # @overload browse(key, timestamp)
        #   View the values that were indexed for _key_ at _timestamp_.
        #   @param [String] key The field name
        #   @param [Integer, String] timestamp The timestamp to use when browsing the index
        #   @return [Hash] A Hash mapping each indexed value to an Array of records where the value was contained at _timestamp_
        # @overload browse(keys)
        #   View the values that are indexed for each of the _keys_.
        #   @param [Array] keys The field names
        #   @return [Hash] A Hash mapping each key to another Hash mapping each indexed value to an Array of records where the value is contained
        # @overload browse(keys, timestamp)
        #   View the values that were indexed for each of the _keys_ at _timestamp_.
        #   @param [Array] keys The field names
        #   @param [Integer, String] timestamp The timestamp to use when browsing each index
        #   @return [Hash] A Hash mapping each key to another Hash mapping each indexed value to an Array of records where the value was contained at _timestamp_
        def browse(*args, **kwargs)
            keys, timestamp = args
            keys ||= kwargs[:keys]
            keys ||= kwargs[:key]
            timestamp ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            timestr = timestamp.is_a? String
            if keys.is_a? Array and !timestamp
                data = @client.browseKeys keys, @creds, @transaction, @environment
            elsif keys.is_a? Array and timestamp and !timestr
                data = @client.browseKeysTime keys, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? Array and timestamp and timestr
                data = @client.browseKeysTimestr keys, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and !timestamp
                data = @client.browseKey keys, @creds, @transaction, @environment
            elsif keys.is_a? String and timestamp and !timestr
                data = @client.browseKeyTime keys, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and timestamp and timestr
                data = @client.browseKeyTimestr keys, timestamp, @creds, @transaction, @environment
            else
                Utils::Args::require 'key or keys'
            end
            return data.rubyify
        end

        # Return a timeseries that shows the state of a field after each change
        # @return [Hash]
        # @overload chronologize(key, record)
        #   Return a timeseries that shows the state of a field after every change.
        #   @param [String] key The field name
        #   @param [Integer] record The record that contains the field
        #   @return [Hash] A Hash mapping a timestamp to all the values that we contained in the field at that timestamp
        # @overload chronologize(key, record, start)
        #   Return a timeseries that shows the state of a field after every change since _start_.
        #   @param [String] key The field name
        #   @param [Integer] record The record that contains the field
        #   @param [Integer, String] start The first timestamp to include in the timeseries
        #   @return [Hash] A Hash mapping a timestamp to all the values that we contained in the field at that timestamp
        # @overload chronologize(key, record, start, end)
        #   Return a timeseries that shows the state of a field after every change between _start_ and _end_.
        #   @param [String] key The field name
        #   @param [Integer] record The record that contains the field
        #   @param [Integer, String] start The first timestamp to include in the timeseries
        #   @param [Integer, String] end The last timestamp to include in the timeseries
        #   @return [Hash] A Hash mapping a timestamp to all the values that we contained in the field at that timestamp
        def chronologize(*args, **kwargs)
            key, record, start, tend = args
            key ||= kwargs[:key]
            record ||= kwargs[:record]
            start ||= kwargs[:start]
            start ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            startstr = start.is_a? String
            tend ||= kwargs[:end]
            endstr = tend.is_a? String
            if !key and !record
                Utils::Args::require 'key and record'
            elsif start and !startstr and tend and !endstr
                data = @client.chronologizeKeyRecordStartEnd key, record, start, tend, @creds, @transaction, @environment
            elsif start and startstr and tend and endstr
                data = @client.chronologizeKeyRecordStartstrEndstr key, record, start, tend, @creds, @transaction, @environment
            elsif start and !startstr
                data = @client.chronologizeKeyRecordStart key, record, start, @creds, @transaction, @environment
            elsif start and startstr
                data = @client.chronologizeKeyRecordStartstr key, record, start, @creds, @transaction, @environment
            else
                data = @client.chronologizeKeyRecord key, record, @creds, @transaction, @environment
            end
            return data.rubyify
        end

        # @overload clear(key, record)
        # @overload clear(key, records)
        # @overload clear(keys, record)
        # @overload clear(keys, records)
        # @overload clear(record)
        # @overload clear(records)
        def clear(*args, **kwargs)
            keys, records = args
            keys ||= kwargs[:keys]
            keys ||= kwargs[:key]
            records ||= kwargs[:records]
            records ||= kwargs[:record]
            if keys.is_a? Array and records.is_a? Array
                @client.clearKeysRecords keys, records, @creds, @transaction, @environment
            elsif keys.nil? and records.is_a? Array
                @client.clearRecords records, @creds, @transaction, @environment
            elsif keys.is_a? Array and records.is_a? Integer
                @client.clearKeysRecord keys, records, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Array
                @client.clearKeyRecords keys, records, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Integer
                @client.clearKeyRecord keys, records, @creds, @transaction, @environment
            elsif keys.nil? and records.is_a? Integer
                @client.clearRecord records, @creds, @transaction, @environment
            else
                Utils::Args::require 'record(s)'
            end
        end

        # Atomically remove data.
        # @return [Void]
        # @overload clear(key, record)
        #   Atomically remove all the values from a field in a single _record_.
        #   @param [String] key The field name
        #   @param [Integer] record The record that contains the field
        #   @return [Void]
        # @overload clear(keys, records)
        #   Atomically remove all the values from multiple fields in multiple _records_.
        #   @param [Array] keys The field names
        #   @param [Array] records The records that contain the field
        #   @return [Void]
        # @overload clear(keys, record)
        #   Atomically remove all the values from multiple fields in a single _record_.
        #   @param [Array] keys The field names
        #   @param [Integer] record The record that contains the field
        #   @return [Void]
        # @overload clear(key, records)
        #   Atomically remove all the values from a field in multiple _records_.
        #   @param [String] key The field name
        #   @param [Array] records The records that contain the field
        #   @return [Void]
        # @overload clear(record)
        #   Atomically remove all the data from a single _record_.
        #   @param [Integer] record The record that contains the field
        #   @return [Void]
        # @overload clear(records)
        #   Atomically remove all the data from multiple _records_.
        #   @param [Array] records The records that contain the field
        #   @return [Void]
        def clear(*args, **kwargs)
            keys, records = args
            keys ||= kwargs[:keys]
            keys ||= kwargs[:key]
            records ||= kwargs[:records]
            records ||= kwargs[:record]

            # If only one arg is supplied it must be record/s
            if !keys.nil? and records.nil?
                records = keys
                keys = nil
            end

            if keys.is_a? Array and records.is_a? Array
                @client.clearKeysRecords keys, records, @creds, @transaction, @environment
            elsif keys.is_a? Array and records.is_a? Integer
                @client.clearKeysRecord keys, records, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Integer
                @client.clearKeyRecord keys, records, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Array
                @client.clearKeyRecords keys, records, @creds, @transaction, @environment
            elsif !keys and records.is_a? Array
                @client.clearRecords records, @creds, @transaction, @environment
            elsif !keys and records.is_a? Integer
                @client.clearRecord records, @creds, @transaction, @environment
            else
                Utils::Args::require 'record or records'
            end
        end

        # TODO
        def close
            self.exit
        end

        # Commit the currently running transaction.
        # @return [Boolean]
        # @raise [TransactionException]
        def commit
            token = @transaction
            @transaction = nil
            if !token.nil?
                return @client.commit @creds, token, @environment
            else
                return false
            end
        end

        # Describe the fields that exist.
        # @return [Array, Hash]
        # @overload describe(record)
        #   Return all the keys in a _record_.
        #   @param [Integer] record The record to describe.
        #   @return [Array] The list of keys
        # @overload describe(record, timestamp)
        #   Return all the keys in a _record_ at _timestamp_.
        #   @param [Integer] record The record to describe.
        #   @param [Integer, String] timestamp The _timestamp_ to use when describing the _record_
        #   @return [Array] The list of keys at _timestamp_
        # @overload describe(records)
        #   Return all the keys in multiple _records_.
        #   @param [Array] records The records to describe.
        #   @return [Hash] A Hash mapping each record to an Array with the list of keys in the record
        # @overload describe(records, timestamp)
        #   Return all the keys in multiple _records_ at _timestamp_.
        #   @param [Array] records The records to describe.
        #   @param [Integer, String] timestamp The _timestamp_ to use when describing each of the _records_
        #   @return [Hash] A Hash mapping each record to an Array with the list of keys in the record at _timestamp_
        def describe(*args, **kwargs)
            records, timestamp = args
            records ||= kwargs[:records]
            records ||= kwargs[:record]
            timestamp ||= kwargs[:timestamp]
            timestamp ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            timestr = timestamp.is_a? String
            if records.is_a? Array and !timestamp
                data = @client.describeRecords records, @creds, @transaction, @environment
            elsif records.is_a? Array and timestamp and !timestr
                data = @client.describeRecordsTime records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Array and timestamp and timestr
                data = @client.describeRecordsTimestr records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Integer and !timestamp
                data = @client.describeRecord records, @creds, @transaction, @environment
            elsif records.is_a? Integer and timestamp and !timestr
                data = @client.describeRecordTime records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Integer and timestamp and timestr
                data = @client.describeRecordTimestr records, timestamp, @creds, @transaction, @environment
            else
                Utils::Args::require 'record or records'
            end
            if data.is_a? Hash
                data.each { |k, v|
                    if v.is_a? Set
                        data[k] = v.to_a
                    end
                }
            elsif data.is_a? Set
                data = data.to_a
            end
            return data
        end

        # Return the differences in data between two timestamps.
        # @return [Hash]
        # @overload diff(key, record, start)
        #   Return the differences in the field between the _start_ and current timestamps.
        #   @param [String] key The field name
        #   @param [Integer] record The record that contains the field
        #   @param [Integer, String] start The timestamp of the original state
        #   @return [Hash] A Hash mapping a description of the change (ADDED OR REMOVED) to an Array of values that match the change.
        # @overload diff(key, record, start, end)
        #   Return the differences in the field between the _start_ and _end_ timestamps.
        #   @param [String] key The field name
        #   @param [Integer] record The record that contains the field
        #   @param [Integer, String] start The timestamp of the original state
        #   @param [Integer, String] end The timestamp of the changed state
        #   @return [Hash] A Hash mapping a description of the change (ADDED OR REMOVED) to an Array of values that match the change.
        # @overload diff(key, start)
        #   Return the differences in the index between the _start_ and current timestamps.
        #   @param [String] key The index name
        #   @param [Integer, String] start The timestamp of the original state
        #   @return [Hash] A Hash mapping a description of the change (ADDED OR REMOVED) to an Array of records that match the change.
        # @overload diff(key, start, end)
        #   Return the differences in the index between the _start_ and _end_ timestamps.
        #   @param [String] key The index name
        #   @param [Integer, String] start The timestamp of the original state
        #   @param [Integer, String] end The timestamp of the changed state
        #   @return [Hash] A Hash mapping a description of the change (ADDED OR REMOVED) to an Array of records that match the change.
        # @overload diff(record, start)
        #   Return the differences in the record between the _start_ and current timestamps.
        #   @param [Integer] record The record to diff
        #   @param [Integer, String] start The timestamp of the original state
        #   @return [Hash] A Hash mapping each key in the record to another Hash mapping a description of the change (ADDED OR REMOVED) to an Array of values that match the change.
        # @overload diff(record, start, end)
        #   Return the differences in the record between the _start_ and _end_ timestamps.
        #   @param [Integer] record The record to diff
        #   @param [Integer, String] start The timestamp of the original state
        #   @param [Integer, String] end The timestamp of the changed state
        #   @return [Hash] A Hash mapping each key in the record to another Hash mapping a description of the change (ADDED OR REMOVED) to an Array of values that match the change.
        def diff(*args, **kwargs)
            key, record, start, tend = args
            key ||= kwargs[:key]
            record ||= kwargs[:record]
            start ||= kwargs[:start]
            start ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            tend||= kwargs[:end]
            startstr = start.is_a? String
            endstr = tend.is_a? String
            if key and record and start and !startstr and tend and !endstr
                data = @client.diffKeyRecordStartEnd key, record, start, tend, @creds, @transaction, @environment
            elsif key and record and start and startstr and tend and endstr
                data = @client.diffKeyRecordStartstrEndstr key, record, start, tend, @creds, @transaction, @environment
            elsif key and record and start and !startstr
                data = @client.diffKeyRecordStart key, record, start, @creds, @transaction, @environment
            elsif key and record and start and startstr
                data = @client.diffKeyRecordStartstr key, record, start, @creds, @transaction, @environment
            elsif key and record.nil? and start and !startstr and tend and !endstr
                data = @client.diffKeyStartEnd key, start, tend, @creds, @transaction, @environment
            elsif key and record.nil? and start and startstr and tend and endstr
                data = @client.diffKeyStartstrEndstr key, start, tend, @creds, @transaction, @environment
            elsif key and record.nil? and start and !startstr
                data = @client.diffKeyStart key, start, @creds, @transaction, @environment
            elsif key and record.nil? and start and startstr
                data = @client.diffKeyStartstr key, start, @creds, @tranaction, @environment
            elsif key.nil? and record and start and !startstr and tend and !endstr
                data = @client.diffRecordStartEnd record, start, tend, @creds, @transaction, @environment
            elsif key.nil? and record and start and startstr and tend and endstr
                data = @client.diffRecordStartstrEndstr record, start, tend, @creds, @transaction, @environment
            elsif key.nil? and record and start and !startstr
                data = @client.diffRecordStart record, start, @creds, @transaction, @environment
            elsif key.nil? and record and start and startstr
                data = @client.diffRecordStartstr record, start, @creds, @transaction, @environment
            else
                Utils::Args::require 'start and (record or key)'
            end
            data = data.rubyify
            if data.is_a? Hash
                data.each { |k, v|
                    if v.is_a? Set
                        data[k] = v.to_a
                    end
                }
            elsif data.is_a? Set
                data = data.to_a
            end
            return data
        end

        # TODO
        def exit
            @client.logout @creds, @environment
            @transport.close
        end

        # Find the records that match a criteria.
        # @return [Array] The records that match the criteria
        # @overload find(key, operator, value)
        #   Find the records where the _key_ field contains at least one value that satisfies _operator_ in relation to _value_.
        #   @param [String] key The field name
        #   @param [Concourse::Thrift::Operator, String] operator The criteria operator
        #   @param [Object] value The criteria value
        #   @return [Array] The records that match the criteria
        # @overload find(timestamp, key, operator, value)
        #   Find the records where the _key_ field contained at least one value that satisfied _operator_ in relation to _value_ at _timestamp_.
        #   @param [String, Integer] timestamp The timestamp to use when evaluating the criteria
        #   @param [String] key The field name
        #   @param [Concourse::Thrift::Operator, String] operator The criteria operator
        #   @param [Object] value The criteria value
        #   @return [Array] The records that match the criteria
        # @overload find(key, operator, values)
        #   Find the records where the _key_ field contains at least one value that satisfies _operator_ in relation to the _values_.
        #   @param [String] key The field name
        #   @param [Concourse::Thrift::Operator, String] operator The criteria operator
        #   @param [Array, Object...] value The criteria values
        #   @return [Array] The records that match the criteria
        # @overload find(timestamp, key, operator, values)
        #   Find the records where the _key_ field contains at least one value that satisfies _operator_ in relation to the _values_ at _timestamp_.
        #   @param [String, Integer] timestamp The timestamp to use when
        #   @param [String] key The field name
        #   @param [Concourse::Thrift::Operator, String] operator The criteria operator
        #   @param [Object] value The criteria value
        #   @return [Array] The records that match the criteria
        # @overload find(criteria)
        #   Find the records that match the _criteria_.
        #   @param [String] criteria The criteria to match
        #   @return [Array] The records that match the criteria
        def find(*args, **kwargs)
            if args.length == 1
                # If there is only one arg, it must be a criteria
                criteria = args.first
            end
            criteria ||= kwargs[:criteria]
            criteria ||= Utils::Args::find_in_kwargs_by_alias 'criteria', kwargs
            key = kwargs[:key]
            operator = kwargs[:operator]
            values = kwargs[:value]
            values ||= kwargs[:values]
            values = [values] unless values.nil? or values.is_a? Array
            values = values.thriftify unless values.nil?
            timestamp = kwargs[:timestamp]
            timestamp ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            operatorstr = operator.is_a? String
            timestr = timestamp.is_a? String
            if !criteria.nil?
                data = @client.findCcl(criteria, @creds, @transaction, @environment)
            elsif key and operator and !operatorstr and !timestamp and values
                data = @client.findKeyOperatorValues key, operator, values, @creds, @transaction, @environment
            elsif key and operator and operatorstr and !timestamp and values
                data = @client.findKeyOperatorstrValues key, operator, values, @creds, @transaction, @environment
            elsif key and operator and !operatorstr and timestamp and !timestr and values
                data = @client.findKeyOperatorValuesTime key, operator, values, timestamp, @creds, @transaction, @environment
            elsif key and operator and operatorstr and timestamp and !timestr and values
                data = @client.findKeyOperatorstrValuesTime key, operator, values, timestamp, @creds, @transaction, @environment
            elsif key and operator and !operatorstr and timestamp and timestr and values
                data = @client.findKeyOperatorValuesTimestr key, operator, values, timestamp, @creds, @transaction, @environment
            elsif key and operator and operatorstr and timestamp and timestr and values
                data = @client.findKeyOperatorstrValuesTimestr key, operator, values, timestamp, @creds, @transaction, @environment
            else
                Utils::Args::require 'criteria or all of (key, operator, value(s))'
            end
            data = data.to_a
            return data
        end

        # Get the most recently added value.
        # @return [Hash, Object]
        # @overload get(key, criteria)
        #   Get the most recently added value from the field in every record that matches the _criteria_.
        #   @param [String] key The field name
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @return [Hash] A mapping from the id of each record to the most recently added value in the field
        # @overload get(key, criteria, timestamp)
        #   Get the most recently added value at _timestamp_ from the field in every record that matches the _criteria_.
        #   @param [String] key The field name
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @param [Integer, String] timestamp The timestamp to use when selecting the data
        #   @return [Hash] A mapping from the id of each record to the most recently added value in the field
        # @overload get(keys, criteria)
        #   Get the most recently added value from each field in every record that matches the _criteria_.
        #   @param [Array] keys The field names
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @return [Hash] A mapping from the id of each record to another Hash mapping the key to the most recently added value in the field
        # @overload get(keys, criteria, timestamp)
        #   Get the most recently added value at _timestamp_ from each field in every record that matches the _criteria_.
        #   @param [Array] keys The field names
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @param [Integer, String] timestamp The timestamp to use when selecting the data
        #   @return [Hash] A mapping from the id of each record to another Hash mapping the key to the most recently added value in the field
        # @overload get(key, record)
        #   Get the most recently added value from the field.
        #   @param [String] key The field name
        #   @param [Integer] record The record to select data from
        #   @return [Object] The most recently added value
        # @overload get(key, record, timestamp)
        #   Get the most recently added value from the field at _timestamp_.
        #   @param [String] key The field name
        #   @param [Integer] record The record to select data from
        #   @param [Integer, String] timestamp The timestamp to use when selecting the data
        #   @return [Object] The most recently added value
        # @overload get(keys, record)
        #   Get the most recently added value from each field.
        #   @param [Array] keys The field name
        #   @param [Integer] record The record to select from
        #   @return [Hash] A Hash mapping each key to the most recently added value in the field
        # @overload get(keys, record, timestamp)
        #   Get the most recently added value from each field at _timestamp_.
        #   @param [Array] keys The field names
        #   @param [Integer] record The record to select data from
        #   @param [Integer, String] timestamp The timestamp to use when selecting the data
        #   @return [Hash] A Hash mapping each key to the most recently added value in the field
        # @overload get(keys, records)
        #   Get the most recently added values from each field in each record.
        #   @param [Array] keys The field names
        #   @param [Array] records The records to select from
        #   @return [Hash] A Hash mapping each record to another Hash mapping each key to the most recently added value in the field
        # @overload get(keys, records, timestamp)
        #   Get the most recently added value from each field in each record at _timestamp_.
        #   @param [Array] keys The field names
        #   @param [Array] records The records to select from
        #   @param [Integer, String] timestamp The timestamp to use when
        #   @return [Hash] A Hash mapping each record to another Hash mapping each key to the most recently added value in the field
        def get(*args, **kwargs)
            keys, criteria, records, timestamp = args
            criteria ||= Utils::Args::find_in_kwargs_by_alias('criteria', kwargs)
            keys ||= kwargs[:keys]
            keys ||= kwargs[:key]
            records ||= kwargs[:records]
            records ||= kwargs[:record]
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
            elsif keys.is_a? Array and records.is_a? Integer and !timestamp.nil? and timestr
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
            elsif keys.is_a? String and !criteria.nil? and !timestamp.nil? and !timestr
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
            elsif keys.is_a? String and records.is_a? Integer and !timestamp.nil? and timestr
                data = @client.getKeyRecordTimestr keys, records, timestamp, @creds, @transaction, @environment
            else
                Utils::Args::require('record or (key and criteria)')
            end
            return data.rubyify
        end

        # Find and return the unique record where the _key_ equals _value_, if
        # it exists. If no record matches, then add _key_ as _value_ in a new
        # record and return the id. If multiple records match the condition, a
        # DuplicateEntryException is raised.
        #
        # This method can be used to simulate a unique index because it
        # atomically checks for a condition and only adds data if the condition
        # isn't currently satisified. If you want to simulate a unique compound
        # index, see the #find_or_insert method, which lets you check a complex
        # criteria.
        # @overload find_or_add(key, value)
        #   @param [String] key The field name
        #   @param [Object] value The Value
        #   @return [Integer] The unique record where _key_ = _value_, if it exists or the new record where _key_ as _value_ is added
        #   @raise [DuplicateEntryException]
        def find_or_add(*args, **kwargs)
            key, value = args
            key ||= kwargs[:key]
            value ||= kwargs[:value]
            value = value.to_thrift
            return @client.findOrAddKeyValue key, value, @creds, @transaction, @environment
        end

        # Find and return the unique record that matches the _criteria_, if it
        # exists. If no record matches, then insert _data_ in a new record and
        # return the id. If multiple records match the _criteria_, a
        # DuplicateEntryException is raised.
        #
        # This method can be used to simulate a unique index because it
        # atomically checks for a condition and only inserts data if the
        # condition isn't currently satisfied.
        # @overload find_or_insert(criteria, data)
        #   @param [String] criteria The unique criteria to find
        #   @param [Hash, Array, String] data The data to insert
        #   @return [Integer] The unique record that matches the _criteria_, if it exists or the new record where _data_ is inserted
        #   @raise [DuplicateEntryException]
        def find_or_insert(*args, **kwargs)
            criteria, data = args
            criteria ||= kwargs[:criteria]
            criteria ||= Utils::Args::find_in_kwargs_by_alias 'criteria', kwargs
            data ||= kwargs[:data]
            data ||= kwargs[:json]
            if data.is_a? Hash or data.is_a? Array
                data = data.to_json
            end
            if criteria and data
                return @client.findOrInsertCclJson criteria, data, @creds, @transaction, @environment
            else
                Utils::Args::require 'criteria and data'
            end
        end

        # Return the environment to which the client is connected.
        # @return [String] the server environment associated with this connection
        def get_server_environment
            return @client.getServerEnvironment @creds, @transaction, @environment
        end

        # Return the version of Concourse Server to which the client is
        # connected. Generally speaking, a client cannot talk to a newer version
        # of Concourse Server.
        # @return [String] the server version
        def get_server_version
            return @client.getServerVersion
        end

        # Atomically bulk insert data. This operation is atomic, within each
        # record which means that an insert will only succeed in a record if all
        # the data can be successfully added, which means an insert will fail in
        # a record if any of the data is already contained.
        # @return [Array, Boolean, Hash]
        # @overload insert(data)
        #   Insert data into one or more new records.
        #   @param [Hash, String] data The Hash or JSON formatted data to insert
        #   @return [Array] An array of new records where the data was added. The array will contain a record id for each JSON object or Hash that was in the blob
        # @overload insert(data, record)
        #   Insert data into the _record_.
        #   @param [Hash, String] data The Hash or JSON formatted data to insert
        #   @param [Integer] record The record where the data is inserted
        #   @return [Boolean] A flag that indicates whether the data was inserted into the _record_
        # @overload insert(data, records)
        #   Insert data into each of the _records_.
        #   @param [Hash, String] data The Hash or JSON formatted data to insert
        #   @param [Array] records The records where the data is inserted
        #   @return [Hash] A hash mapping each record to a flag that indicates whether the data inserted into the record.
        def insert(*args, **kwargs)
            data, records = args
            data ||= kwargs[:data]
            data ||= Utils::Args::find_in_kwargs_by_alias 'json', kwargs
            records ||= kwargs[:records]
            records ||= kwargs[:record]
            if data.is_a? Hash or data.is_a? Array
                data = data.to_json
            end
            if records.is_a? Array
                result = @client.insertJsonRecords data, records, @creds, @transaction, @environment
            elsif records.is_a? Integer
                result = @client.insertJsonRecord data, records, @creds, @transaction, @environment
            elsif !data.nil?
                result = @client.insertJson data, @creds, @transaction, @environment
            else
                Utils::Args::require 'data'
            end
            if result.is_a? Set
                result = result.to_a
            end
            return result
        end

        # Return all the records that have current or historical data.
        # @return [Array] All the records
        def inventory
            data = @client.inventory @creds, @transaction, @environment
            return data.to_a
        end

        # Export data to a JSON string.
        # @return [String] The JSON string containing the data
        # @overload jsonify(record)
        #   Return a JSON string that contains all the data in _record_.
        #   @param [Integer] record The record to export
        #   @return [String] The JSON string containing the data
        # @overload jsonify(record, timestamp)
        #   Return a JSON string that contains all the data in _record_ at _timestamp_.
        #   @param [Integer] record The record to export
        #   @param [Integer, String] timestamp The timestamp to use when exporting the record
        #   @return [String] The JSON string containing the data
        # @overload jsonify(record, include_id)
        #   Return a JSON string that contains all the data in _record_ and optionally include the record id in the dump. This option is useful when you want to dump a record from one instance and import it into another with the same id.
        #   @param [Integer] record The record to export
        #   @param [Boolean] include_id A flag that determines if the record id should be included
        #   @return [String] The JSON string containing the data
        # @overload jsonify(record, timestamp, include_id)
        #   Return a JSON string that contains all the data in _record_ at _timestamp_ and optionally include the record id in the dump. This option is useful when you want to dump a record from one instance and import it into another with the same id.
        #   @param [Integer] record The record to export
        #   @param [String, Integer] timestamp The timestamp to use when exporting the record
        #   @param [Boolean] include_id A flag that determines if the record id should be included
        #   @return [String] The JSON string containing the data
        # @overload jsonify(records)
        #   Return a JSON string that contains all the data in each of the _records_.
        #   @param [Array] records The records to export
        #   @return [String] The JSON string containing the data
        # @overload jsonify(records, timestamp)
        #   Return a JSON string that contains all the data in each of the _records_ at _timestamp_.
        #   @param [Array] records The records to export
        #   @param [Integer, String] timestamp The timestamp to use when exporting the records
        #   @return [String] The JSON string containing the data
        # @overload jsonify(records, include_id)
        #   Return a JSON string that contains all the data in each of the _records_ and optionally include the record ids in the dump. This option is useful when you want to dump records from one instance and import them into another with the same ids.
        #   @param [Array] records The records to export
        #   @param [Boolean] include_id A flag that determines if the record ids should be included
        #   @return [String] The JSON string containing the data
        # @overload jsonify(records, timestamp, include_id)
        #   Return a JSON string that contains all the data in each of the _records_ at _timestamp_ and optionally include the record ids in the dump. This option is useful when you want to dump records from one instance and import them into another with the same ids.
        #   @param [Array] records The records to export
        #   @param [String, Integer] timestamp The timestamp to use when exporting the records
        #   @param [Boolean] include_id A flag that determines if the record ids should be included
        #   @return [String] The JSON string containing the data
        def jsonify(*args, **kwargs)
            records, timestamp, include_id = args
            records ||= kwargs[:records]
            records ||= kwargs[:record]
            records = records.to_a
            include_id ||= kwargs[:include_id]
            include_id ||= false
            timestamp ||= kwargs[:timestamp]
            timestamp ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            timestr = timestamp.is_a? String
            if !timestamp
                return @client.jsonifyRecords records, include_id, @creds, @transaction, @environment
            elsif timestamp and !timestr
                return @client.jsonifyRecordsTime records, timestamp, include_id, @creds, @transaction, @environment
            elsif timestamp and timestr
                return @client.jsonifyRecordsTimestr records, timestamp, include_id, @creds, @transaction, @environment
            else
                Utils::Args::require 'record(s)'
            end
        end

        # @overload link(key, source, destination)
        # @overload link(key, source, destinations)
        def link(*args, **kwargs)
            key, source, destinations = args
            key ||= kwargs[:key]
            source ||= kwargs[:source]
            destinations ||= kwargs[:destinations]
            destinations ||= kwargs[:destination]
            if key and source and destinations.is_a? Array
                data = {}
                destinations.each { |x| data[x] = self.add(key:key, value:Link.to(x), record:source)}
                return data
            elsif key and source and destinations.is_a? Integer
                return self.add(key:key, value:Link.to(destinations), record:source)
            else
                Utils::Args::require 'key, source and destination(s)'
            end
        end

        # An internal method that allows unit tests to "logout" from the server
        # without closing the transport. This should only be used in unit tests
        # that are connected to Mockcourse.
        # @!visibility private
        def logout()
            @client.logout(@creds, @environment)
        end

        # Check if data currently exists.
        # @return [Boolean, Hash]
        # @overload ping(record)
        #   Check to see if the _record_ currently has data.
        #   @param [Integer] record The record to ping
        #   @return [Boolean] A flag that indiciates whether the record currently has data
        # @overload ping(records)
        #   Check to see if each of the _records_ currently has data.
        #   @param [Array] records The records to ping
        #   @return [Hash] A Hash mapping each record to a boolean that indicates whether the record currently has data
        def ping(*args, **kwargs)
            records = args.first
            records ||= kwargs[:records]
            records ||= kwargs[:record]
            if records.is_a? Array
                return @client.pingRecords records, @creds, @transaction, @environment
            elsif records.is_a? Integer
                return @client.pingRecord records, @creds, @transaction, @environment
            else
                Utils::Args::require 'record(s)'
            end
        end

        # Remove a value if it exists.
        # @return [Boolean, Hash]
        # @overload remove(key, value, record)
        #   Remove a value from a field in a single record.
        #   @param [String] key The field name
        #   @param [Object] value The value to remove
        #   @param [Integer] record The record that contains the data
        #   @return [Boolean] A flag that indicates whether the value was removed
        # @overload remove(key, value, records)
        #   Remove a value from a field in multiple records.
        #   @param [String] key The field name
        #   @param [Object] value The value to remove
        #   @param [Array] records The records that contain the data
        #   @return [Hash] A Hash mapping the record id to a Boolean that indicates whether the value was removed
        def remove(*args, **kwargs)
            key, value, records = args
            key ||= kwargs.fetch(:key, key)
            value ||= kwargs.fetch(:value, value)
            records ||= kwargs.fetch(:record, nil)
            records ||= kwargs.fetch(:records, nil)
            value = value.to_thrift
            if records.is_a? Array
                return @client.removeKeyValueRecords key, value, records, @creds, @transaction, @environment
            elsif records.is_a? Integer
                return @client.removeKeyValueRecord key, value, records, @creds, @transaction, @environment
            else
                Utils::Args::require 'record or records'
            end
        end

        # @overload revert(key, record, timestamp)
        # @overload revert(keys, record, timestamp)
        # @overload revert(key, records, timestamp)
        # @overload revert(keys, records, timestamp)
        def revert(*args, **kwargs)
            keys, records, timestamp = args
            keys ||= kwargs[:keys]
            keys ||= kwargs[:key]
            records ||= kwargs[:records]
            records ||= kwargs[:record]
            timestamp ||= kwargs[:timestamp]
            timestamp ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            timestr = timestamp.is_a? String
            if keys.is_a? Array and records.is_a? Array and timestamp and !timestr
                @client.revertKeysRecordsTime keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? Array and records.is_a? Array and timestamp and timestr
                @client.revertKeysRecordsTimestr keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? Array and records.is_a? Integer and timestamp and !timestr
                @client.revertKeysRecordTime keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? Array and records.is_a? Integer and timestamp and timestr
                @client.revertKeysRecordTimestr keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and record.is_a? Array and timestamp and !timestr
                @client.revertKeyRecordsTime keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Array and timestamp and timestr
                @client.revertKeyRecordsTimestr keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Integer and timestamp and !timestr
                @client.revertKeyRecordTime keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Integer and timestamp and timestr
                @client.revertKeyRecordTimestr keys, records, timestamp, @creds, @transaction, @environment
            else
                Utils::Args::require 'keys, record and timestamp'
            end
        end

        # @overload search(key, query)
        def search(*args, **kwargs)
            key, query = args
            key ||= kwargs[:key]
            query ||= kwargs[:query]
            if key.is_a? String and query.is_a? String
                data = @client.search key, query, @creds, @transaction, @environment
            else
                Utils::Args::require 'key and query'
            end
            data = data.to_a
            return data
        end

        # Select all values.
        # @return [Hash, Array]
        # @overload select(criteria)
        #   Select all the data from every record that matches the _criteria_.
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @return [Hash] A mapping from the id of each record to another Hash mapping each key to a Array containing all the values
        # @overload select(criteria, timestamp)
        #   Select all the data at _timestamp_ from every record that matches the _criteria_.
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @param [Integer, String] timestamp The timestamp to use when selecting the data
        #   @return [Hash] A mapping from the id of each record to anothe Hash mapping each key to a Array containing all the values
        # @overload select(key, criteria)
        #   Select all the values from the field in every record that matches the _criteria_.
        #   @param [String] key The field name
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @return [Hash] A mapping from the id of each record to a Array containing all the values in the field
        # @overload select(key, criteria, timestamp)
        #   Select all the values at _timestamp_ from the field in every record that matches the _criteria_.
        #   @param [String] key The field name
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @param [Integer, String] timestamp The timestamp to use when selecting the data
        #   @return [Hash] A mapping from the id of each record to a Array containing all the values in the field
        # @overload select(keys, criteria)
        #   Select all the values from each field in every record that matches the _criteria_.
        #   @param [Array] keys The field names
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @return [Hash] A mapping from the id of each record to another Hash mapping the key to a Array containing all the values in the field
        # @overload select(keys, criteria, timestamp)
        #   Select all the values at _timestamp_ from each field in every record that matches the _criteria_.
        #   @param [Array] keys The field names
        #   @param [String] criteria The criteria that determines which records are relevant
        #   @param [Integer, String] timestamp The timestamp to use when selecting the data
        #   @return [Hash] A mapping from the id of each record to another Hash mapping the key to a Array containing all the values in the field
        # @overload select(key, record)
        #   Select all the values from the field.
        #   @param [String] key The field name
        #   @param [Integer] record The record to select data from
        #   @return [Array] An Array containing all the values
        # @overload select(key, record, timestamp)
        #   Select all the values from the field at _timestamp_.
        #   @param [String] key The field name
        #   @param [Integer] record The record to select data from
        #   @param [Integer, String] timestamp The timestamp to use when selecting the data
        #   @return [Array] An Array containing all the values
        # @overload select(keys, record)
        #   Select all the values from each field.
        #   @param [Array] keys The field name
        #   @param [Integer] record The record to select from
        #   @return [Hash] A Hash mapping each key to a Array containing all the values in the field
        # @overload select(keys, record, timestamp)
        #   Select all the values from each field at _timestamp_.
        #   @param [Array] keys The field names
        #   @param [Integer] record The record to select data from
        #   @param [Integer, String] timestamp The timestamp to use when
        #   @return [Hash] A Hash mapping each key to a Array containing all the values in the field
        # @overload select(keys, records)
        #   Select all the values from each field in each record.
        #   @param [Array] keys The field names
        #   @param [Array] records The records to select from
        #   @return [Hash] A Hash mapping each record to another Hash mapping each key to a Array containing all the values in the field
        # @overload select(keys, records, timestamp)
        #   Select all the values from each field in each record at _timestamp_.
        #   @param [Array] keys The field names
        #   @param [Array] records The records to select from
        #   @param [Integer, String] timestamp The timestamp to use when
        #   @return [Hash] A Hash mapping each record to another Hash mapping each key to a Array containing all the values in the field
        # @overload select(record)
        #   Select all the data from the record.
        #   @param [Integer] record The record to select from
        #   @return [Hash] A mapping from each key in the record to a Array containing all the values in the field
        # @overload select(record, timestamp)
        #   Select all the data from the record at _timestamp_.
        #   @param [Integer] record The record to select from
        #   @param [Integer, String] timestamp The timestamp to use when
        #   @return [Hash] A mapping from each key in the record to a Array containing all the values in the field
        # @overload select(records)
        #   Select all the data from each record.
        #   @param [Array] records The records to select from
        #   @return [Hash] A hash mapping each record to another Hash mapping each key in the record to a Array containing all the values in the field
        # @overload select(records, timestamp)
        #   Select all the data from each record at _timestamp_.
        #   @param [Array] records The records to select from
        #   @param [Integer, String] timestamp The timestamp to use when
        #   @return [Hash] A hash mapping each record to another Hash mapping each key in the record to a Array containing all the values in the field
        def select(*args, **kwargs)
            keys, criteria, records, timestamp = args
            criteria ||= Utils::Args::find_in_kwargs_by_alias('criteria', kwargs)
            keys ||= kwargs.fetch(:key, nil)
            records = records ||= kwargs.fetch(:record, nil)
            timestamp ||= Utils::Args::find_in_kwargs_by_alias('timestamp', kwargs)
            timestr = timestamp.is_a? String

            # If there is only one argument and it is an array or an integer,
            # then it must be records
            if criteria.nil? and records.nil? and (keys.is_a? Array or keys.is_a? Integer)
                records = keys
                keys = nil
            # If there is only one argument and it is a tring, then it must
            # criteria
            elsif criteria.nil? and records.nil? and keys.is_a? String
                criteria = keys
                keys = nil
            end

            if records.is_a? Array and !keys and !timestamp
                data = @client.selectRecords records, @creds, @transaction, @environment
            elsif records.is_a? Array and !keys and timestamp and !timestr
                data = @client.selectRecordsTime records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Array and !keys and timestamp and timestr
                data = @client.selectRecordsTimestr records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Array and keys.is_a? Array and !timestamp
                data = @client.selectKeysRecords keys, records, @creds, @transaction, @environment
            elsif records.is_a? Array and keys.is_a? Array and timestamp and !timestr
                data = @client.selectKeysRecordsTime keys, records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Array and keys.is_a? Array and timestamp and timestr
                data = @client.selectKeysRecordsTimestr keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? Array and criteria and !timestamp
                data = @client.selectKeysCcl keys, criteria, @creds, @transaction, @environment
            elsif keys.is_a? Array and criteria and timestamp and !timestr
                data = @client.selectKeysCclTime keys, criteria, @creds, @transaction, @environment
            elsif keys.is_a? Array and criteria and timestamp and timestr
                data = @client.selectKeysCclTimestr keys, criteria, @creds, @transaction, @environment
            elsif keys.is_a? Array and records.is_a? Integer and !timestamp
                data = @client.selectKeysRecord keys, records, @creds, @transaction, @environment
            elsif keys.is_a? Array and records.is_a? Integer and timestamp and !timestr
                data = @client.selectKeysRecordTime keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? Array and records.is_a? Integer and timestamp and timestr
                data = @client.selectKeysRecordTimestr keys, records, timestamp, @creds, @transaction, @environment
            elsif criteria and !keys and !timestamp
                data = @client.selectCcl criteria, @creds, @transaction, @environment
            elsif criteria and !keys and timestamp and !timestr
                data = @client.selectCclTime criteria, timestamp, @creds, @transaction, @environment
            elsif criteria and !keys and timestamp and timestr
                data = @client.selectCclTimestr criteria, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Array and !keys and !timestamp
                data = @client.selectRecords records, @creds, @transaction, @environment
            elsif records.is_a? Array and !keys and timestamp and !timestr
                data = @client.selectRecordsTime records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Array and !keys and timestamp and timestr
                data = @client.selectRecordsTimestr records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Integer and !keys and !timestamp
                data = @client.selectRecord records, @creds, @transaction, @environment
            elsif records.is_a? Integer and !keys and timestamp and !timestr
                data = @client.selectRecordTime records, timestamp, @creds, @transaction, @environment
            elsif records.is_a? Integer and !keys and timestamp and timestr
                data = @client.selectRecordTimestr records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and criteria and !timestamp
                data = @client.selectKeyCcl keys, criteria, @creds, @transaction, @environment
            elsif keys.is_a? String and criteria and timestamp and !timestr
                data = @client.selectKeyCclTime keys, criteria, @creds, @transaction, @environment
            elsif keys.is_a? String and criteria and timestamp and timestr
                data = @client.selectKeyCclTimestr keys, criteria, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Array and !timestamp
                data = @client.selectKeyRecords keys, records, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Array and timestamp and !timestr
                data = @client.selectKeyRecordsTime keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Array and timestamp and timestr
                data = @client.selectKeyRecordsTimestr keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Integer and !timestamp
                data = @client.selectKeyRecord keys, records, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Integer and timestamp and !timestr
                data = @client.selectKeyRecordTime keys, records, timestamp, @creds, @transaction, @environment
            elsif keys.is_a? String and records.is_a? Integer and timestamp and timestr
                data = @client.selectKeyRecordTimestr keys, records, timestamp, @creds, @transaction, @environment
            else
                Utils::Args::require 'criteria or record'
            end
            return data.rubyify
        end

        # Atomically remove all existing values from a field and add a new one.
        # @return [Void, Integer]
        # @overload set(key, value, record)
        #   Atomically remove all the values from a field in a record and add a new value.
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @param [Integer] record The record where the data is added
        #   @return [Void]
        # @overload set(key, value, records)
        #   Atomically remove all the values from a field in multiple records and add a new value.
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @param [Array] records The records where the data is added
        #   @return [Void]
        # @overload set(key, value)
        #   Add a value to a field in a new record.
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @return [Integer] The id of the new record where the data was added
        def set(*args, **kwargs)
            key, value, records = args
            records ||= kwargs[:record]
            value = value.to_thrift
            if !records
                return @client.setKeyValue key, value, @creds, @transaction, @environment
            elsif records.is_a? Array
                return @client.setKeyValueRecords key, value, records, @creds, @transaction, @environment
            elsif records.is_a? Integer
                return @client.setKeyValueRecord key, value, records, @creds, @transaction, @environment
            else
                Utils::Args::require 'record or records'
            end
        end

        # Start a transaction.
        #
        # This method will turn on _staging_ mode so that all subsequent changes
        # are collected in an isolated buffer before possibly being committed.
        # Staged operations are guaranteed to be reliable, all or nothing, units
        # of work that allow correct recovery from failures and provide
        # isolation between clients so the database is always consistent.
        #
        # After this method returns, all subsequent operations will be done in
        # _staging_ mode until either #commit or #abort is called.
        #
        # All operations that occur within a transaction should be wrapped in a
        # begin-rescue block so that transaction exceptions can be caught and
        # the application can decided to abort or retry the transaction:
        #
        #   concourse.stage
        #   begin
        #       concourse.get key:"name", record:1
        #       concourse.add key:"name", value:"Jeff Nelson", record:1
        #       concourse.commit
        #   rescue Concourse::TransactionException
        #       concourse.abort
        #   end
        #
        # @return [Void]
        def stage
            @transaction = @client.stage @creds, @environment
        end

        # Return a unix timestamp in microseconds.
        # @return [Integer]
        # @overload time
        #   @return [Integer] The current unix timestamp in microseconds
        # @overload time(phrase)
        #   @param [String] phrase A natural language phrase that describes the desired timestamp (i.e. 3 weeks ago, last month, yesterday at 3:00pm, etc)
        #   @return [Integer] The unix timestamp that corresponds to the phrase
        def time(*args, **kwargs)
            phrase = args.first
            phrase ||= kwargs[:phrase]
            if phrase
                return @client.timePhrase phrase, @creds, @transaction, @environment
            else
                return @client.time @creds, @transaction, @environment
            end
        end

        # @overload unlink(key, source, destination)
        # @overload unline(key, source, destinations)
        def unlink(*args, **kwargs)
            key, source, destinations = args
            key ||= kwargs[:key]
            source ||= kwargs[:source]
            destinations ||= kwargs[:destinations]
            destinations ||= kwargs[:destination]
            if key and source and destinations.is_a? Array
                data = {}
                destinations.each { |x| data[x] = self.remove(key:key, value:Link.to(x), record:source)}
                return data
            elsif key and source and destinations.is_a? Integer
                return self.remove(key:key, value:Link.to(destinations), record:source)
            else
                Utils::Args::require 'key, source, and destination(s)'
            end
        end

        # @overload verify(key, value, record)
        # @overload verify(key, value, record, timestamp)
        def verify(*args, **kwargs)
            key, value, record, timestamp = args
            key ||= kwargs[:key]
            value ||= kwargs[:value]
            record ||= kwargs[:record]
            timestamp ||= kwargs[:timestamp]
            timestamp ||= Utils::Args::find_in_kwargs_by_alias 'timestamp', kwargs
            timestr = timestamp.is_a? String
            value = value.to_thrift
            if key and value and record and !timestamp
                return @client.verifyKeyValueRecord key, value, record, @creds, @transaction, @environment
            elsif key and value and record and timestamp and !timestr
                return @client.verifyKeyValueRecordTime key, value, record, timestamp, @creds, @transaction, @environment
            elsif key and value and record and timestamp and timestr
                return @client.verifyKeyValueRecordTimestr key, value, record, timestamp, @creds, @transaction, @environment
            else
                Utils::Args::require 'key, value, and record'
            end
        end

        # Atomically verify the existence of a value in a field within a record
        # and swap that value with a new one.
        # @overload verify_and_swap(key, expected, record, replacement)
        #   @param [String] key The field name
        #   @param [Object] expected The value to check for
        #   @param [Integer] record The record that contains the field
        #   @param [Object] replacement The value to swap in
        #   @return [Boolean] Returns _true_ if and only if the both the verification and swap are successful
        def verify_and_swap(*args, **kwargs)
            key, expected, record, replacement = args
            key ||= kwargs[:key]
            expected ||= kwargs[:expected]
            expected ||= Utils::Args::find_in_kwargs_by_alias 'expected', kwargs
            replacement ||= kwargs[:replacement]
            replacement ||= Utils::Args::find_in_kwargs_by_alias 'replacement', kwargs
            expected = expected.to_thrift
            replacement = replacement.to_thrift
            if key and expected and record and replacement
                return @client.verifyAndSwap key, expected, record, replacement, @creds, @transaction, @environment
            else
                Utils::Args::require 'key, expected, record, and replacement'
            end
        end

        # Atomically verify that a field contains a single particular value or
        # set it as such.
        #
        # Please note that after returning, this method guarantees that _key_ in # _record_ will only contain _value_, even if it already existed
        # alongside other values
        # (e.g. calling concourse.verify_or_set("foo", "bar", 1) will mean that
        # the field named "foo" in record 1 will only have "bar" as a value
        # after returning, even if the field already contained "bar", "baz" and
        # "apple" as values.
        #
        # Basically, this method has the same guarantee as the [#set] method,
        # except it will not create any new revisions unless it is necessary
        # to do so. The [#set] method, on the other hand, would indiscriminately
        # clear all the values in the field before adding _value_, even if
        # _value_ already existed.
        #
        # If you want to add a value that does not exist, while also preserving
        # other values that also exist in the field, you should use the [#add]
        # method instead.
        # @overload verify_or_set(key, value, record)
        #   @param [String] key The field name
        #   @param [Object] value The value ensure exists solely in the field
        #   @param [Integer] record The record that contains the field
        #   @return [Void]
        def verify_or_set(*args, **kwargs)
            key, value, record = args
            key ||= kwargs[:key]
            value ||= kwargs[:value]
            record ||= kwargs[:record]
            value = value.to_thrift
            @client.verifyOrSet key, value, record, @creds, @transaction, @environment
        end

        # Internal method to login with @username and @password and locally
        # store the AccessToken for use with subsequent operations.
        def authenticate()
            begin
                @creds = @client.login(@username, @password, @environment)
            rescue Thrift::Exception => ex
                raise ex
            end
        end

        # Return string representation of the connection
        # @return [String] The string representation
        # @!visibility private
        def to_s
            return "Connected to #{@host}:#{@port} as #{@username}"
        end

        private :authenticate

    end

    # The base class for all exceptions that happen during (staged) operations
    # in a transaction.
    class TransactionException < RuntimeError

        # Intercept the constructor for TTransactionException and return a
        # TransactionException instead.
        Thrift::TTransactionException.class_eval do

            def initialize
                raise TransactionException
            end
        end

        def initialize
            super "Another client has made changes to data used within the current transaction, so it cannot continue. Please abort the transaction and try again."
        end
    end

    # An exception that is thrown when attempting to conditionally add or Insert
    # data based on a condition that should be unique.
    class DuplicateEntryException < RuntimeError

        # Intercept the constructor for TDuplicateEntryException and return a
        # DuplicateEntryException instead.
        Thrift::TDuplicateEntryException.class_eval do

            def initialize(message=nil)
                raise DuplicateEntryException
            end
        end
    end

end
