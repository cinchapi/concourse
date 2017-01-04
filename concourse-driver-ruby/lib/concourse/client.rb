# Copyright (c) 2013-2017 Cinchapi Inc.
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
require 'concourse/dispatch'
require 'json'

module Concourse

    # This is an alias for the Concourse::Client constructor
    # @return [Concourse::Client] the handle
    def self.connect(host: "localhost", port: 1717, username: "admin", password: "admin", environment: "", **kwargs)
        return Concourse::Client.new(host: host, port: port, username: username, password: password, environment: environment, **kwargs)
    end

    # Concourse is a self-tuning database that is designed for both ad hoc
    # analytics and high volume transactions at scale. Developers use Concourse
    # to quickly build mission critical software while also benefiting from real
    # time insight into their most important data. With Concourse, end-to-end
    # data management requires no extra infrastructure, no prior configuration
    # and no additional coding–all of which greatly reduce costs and allow
    # developers to focus on core business problems.
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

        # Create a new client connection.
        # @param host [String] the server host (optional,
        # defaults to 'localhost')
        # @param port [Integer] the listener port (optional, defaults to 1717)
        # @param username [String] the username with which to connect
        # (optional, defaults to 'admin')
        # @param password [String] the password for the username
        # (optional, defaults to 'admin')
        # @param environment [String] the environment to use,
        # by default the default_environment in the server's concourse.prefs
        # file is used
        # @option kwargs [String] :prefs  You may specify the path to a
        # preferences file using the 'prefs' keyword argument. If a prefs file is supplied, the values contained therewithin for any of the arguments above become the default if those arguments are not explicitly given values.
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

        # Abort the current transaction and discard any changes that are
        # staged.
        #
        # After returning, the driver will return to _autocommit_ mode and
        # all subsequent changes will be committed imediately.
        #
        # Calling this method when the driver is not in _staging_ mode is a
        # no-op.
        # @return [Void]
        def abort
            if !@transaction.nil?
                token = @transaction
                @transaction = nil
                @client.abort @creds, token, @environment
            end
        end

        # Append a _key_ as a _value_ in one or more records.
        # @return [Boolean, Hash, Integer]
        # @overload add(key, value)
        #   Append _key_ as _value_ in a new record and return the id.
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @return [Integer] The new record id
        # @overload add(key, value, record)
        #   Append _key_ as _value_ in _record_ if and only if it doesn't exist.
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @param [Integer] record The record id where an attempt is made to add the data
        #   @return [Boolean] A boolean that indicates if the data was added
        # @overload add(key, value, records)
        #   Atomically Append _key_ as _value_ in each of the _records_ where
        #   it doesn't exist and return an associative array associating each
        #   record id to a boolean that indicates if the data was added
        #   @param [String] key The field name
        #   @param [Object] value The value to add
        #   @param [Array] records An array of record ids where an attempt is made to add the data
        #   @return [Hash] A Hash mapping from each record id to a Boolean that indicates if the data was added
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

        # List changes made to a _field_ or _record_ over time.
        # @return [Hash]
        # @overload audit(key, record)
        #   List all the changes ever made to the _key_ field in _record_.
        #   @param [String] key the field name
        #   @param [Integer] record the record id
        #   @return [Hash] A Hash containing, for each change, a mapping from timestamp to a description of the change that occurred
        # @overload audit(key, record, start)
        #   List all the changes made to the _key_ field in _record_ since _start_ (inclusive).
        #   @param [String] key the field name
        #   @param [Integer] record the record id
        #   @param [Integer, String] start an inclusive timestamp for the oldest change that should possibly be included in the audit
        #   @return [Hash] A Hash containing, for each change, a mapping from timestamp to a description of the change that occurred
        # @overload audit(key, record, start, end)
        #   List all the changes made to the _key_ field in _record_ between _start_ (inclusive) and _end_ (non-inclusive).
        #   @param [String] key the field name
        #   @param [Integer] record the record id
        #   @param [Integer, String] start an inclusive timestamp for the oldest change that should possibly be included in the audit
        #   @param [Integer, String] end a non-inclusive timestamp for the most recent change that should possibly be included in the audit
        #   @return [Hash] A Hash containing, for each change, a mapping from timestamp to a description of the change that occurred
        # @overload audit(record)
        #   List all the changes ever made to _record_.
        #   @param [Integer] record the record id
        #   @return [Hash] A Hash containing, for each change, a mapping from timestamp to a description of the change that occurred
        # @overload audit(record, start)
        #   List all the changes made to _record_ since _start_ (inclusive).
        #   @param [Integer] record the record id
        #   @param [Integer, String] start an inclusive timestamp for the oldest change that should possibly be included in the audit
        #   @return [Hash] A Hash containing, for each change, a mapping from timestamp to a description of the change that occurred
        # @overload audit(record, start, end)
        #   List all the changes made to _record_ between _start_ (inclusive) and _end_ (non-inclusive).
        #   @param [Integer] record the record id
        #   @param [Integer, String] start an inclusive timestamp for the oldest change that should possibly be included in the audit
        #   @param [Integer, String] end a non-inclusive timestamp for the most recent change that should possibly be included in the audit
        #   @return [Hash] A Hash containing, for each change, a mapping from timestamp to a description of the change that occurred
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

        # For one or more _fields_, view the values from all records currently or previously stored.
        # @return [Hash]
        # @overload browse(key)
        #   View the values from all records that are currently stored for _key_.
        #   @param [String] key The field name
        #   @return [Hash] A Hash associating each value to the array of records that contain that value in the _key_ field
        # @overload browse(key, timestamp)
        #   View the values from all records that were stored for _key_ at _timestamp_.
        #   @param [String] key The field name
        #   @param [Integer, String] timestamp The historical timestamp to use in the lookup
        #   @return [Hash] A Hash associating each value to the array of records that contained that value in the _key_ field at _timestamp_
        # @overload browse(keys)
        #   View the values from all records that are currently stored for each of the _keys_.
        #   @param [Array] keys An array of field names
        #   @return [Hash] A Hash associating each key to a Hash associating each value to the array of records that contain that value in the _key_ field
        # @overload browse(keys, timestamp)
        #   View the values from all records that were stored for each of the _keys_ at _timestamp_.
        #   @param [Array] keys An array of field names
        #   @param [Integer, String] timestamp The historical timestamp to use in the lookup
        #   @return [Hash] A Hash associating each key to a Hash associating each value to the array of records that contained that value in the _key_ field at _timestamp_
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

        # View a time series with snapshots of a _field_ after every change.
        # @return [Hash]
        # @overload chronologize(key, record)
        #   View a time series that associates the timestamp of each modification for _key_ in _record_ to a snapshot containing the values that were stored in the field after the change.
        #   @param [String] key The field name
        #   @param [Integer] record The record id
        #   @return [Hash] A Hash associating each modification timestamp to the array of values that were stored in the field after the change
        # @overload chronologize(key, record, start)
        #   View a time series between _start_ (inclusive) and the present that associates the timestamp of each modification for _key_ in _record_ to a snapshot containing the values that were stored in the field after the change.
        #   @param [String] key The field name
        #   @param [Integer] record The record id
        #   @param [Integer, String] start The first possible timestamp to include in the time series
        #   @return [Hash] A Hash associating each modification timestamp to the array of values that were stored in the field after the change
        # @overload chronologize(key, record, start, end)
        #   Return a timeseries that shows the state of a field after every change between _start_ and _end_.
        #   @param [String] key The field name
        #   @param [Integer] record The record id
        #   @param [Integer, String] start The first possible timestamp to include in the time series
        #   @param [Integer, String] end The last timestamp that should be greater than every timestamp in the time series
        #   @return [Hash] A Hash associating each modification timestamp to the array of values that were stored in the field after the change
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

        # Atomically remove all the values from one or more _fields_.
        # @return [Void]
        # @overload clear(record)
        #   Atomically remove all the values stored for every key in _record_.
        #   @param [Array] record The record id
        #   @return [Void]
        # @overload clear(records)
        #   Atomically remove all the values stored for every key in each of the _records_.
        #   @param [Array] records A collection of record ids
        #   @return [Void]
        # @overload clear(key, record)
        #   Atomically remove all the values stored for _key_ in _record_.
        #   @param [String] key The field name
        #   @param [Integer] record The record id
        #   @return [Void]
        # @overload clear(keys, record)
        #   Atomically remove all the values stored for each of the _keys_ in _record_.
        #   @param [Array] keys A collection of field names
        #   @param [Integer] record The record id
        #   @return [Void]
        # @overload clear(key, records)
        #   Atomically remove all the values stored for _key_ in each of the _records_.
        #   @param [String] key The field name
        #   @param [Array] records The record id
        #   @return [Void]
        # @overload clear(keys, records)
        #   Atomically remove all the values stored for each of the _keys_ in each of the _records_.
        #   @param [Array] keys A collection of field names
        #   @param [Array] records A collection of record ids
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

        # Terminate the client's session and close this connection.
        # An alias for the #exit method.
        # @return [Void]
        def close
            self.exit
        end

        # Attempt to permanently commit any changes that are staged in a
        # transaction and return _true_ if and only if all the changes can be
        # applied. Otherwise, returns _false_ and all the changes are discarded.
        #
        # After returning, the driver will return to _autocommit_ mode and all
        # subsequent changes will be committed immediately.
        #
        # This method will return _false_ if it is called when the driver is not
        # in _staging_ mode.
        #
        # @return [Boolean] _true_ if all staged changes are committed, otherwise _false_
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

        # For one or more _records_, list all the _keys_ that have at least one
        # value.
        # @return [Array, Hash]
        # @overload describe(record)
        #   List all the keys in _record_ that have at least one value.
        #   @param [Integer] record The record id
        #   @return [Array] The Array of keys in _record_
        # @overload describe(record, timestamp)
        #   List all the keys in _record_ that had at least one value at
        #   _timestamp_.
        #   @param [Integer] record The record id
        #   @param [Integer, String] timestamp The historical timestamp to use in the lookup
        #   @return [Array] The Array of keys that were in _record_ at _timestamp_
        # @overload describe(records)
        #   For each of the _records_, list all of the keys that have at least
        #   one value.
        #   @param [Array] records An Array of record ids
        #   @return [Hash] A Hash associating each record id to the Array of keys in that record
        # @overload describe(records, timestamp)
        #   For each of the _records_, list all the keys that had at least one
        #   value at _timestamp_.
        #   @param [Array] records An Array of record ids.
        #   @param [Integer, String] timestamp The historical timestamp to use in the lookup
        #   @return [Hash] A Hash associating each record id to the Array of keys that were in that record at _timestamp_
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

        # List the net changes made to a _field_, _record_ or _index_ from one
        # timestamp to another.
        #
        # If you begin with the state of the _record_ at _start_
        # and re-apply all the changes in the diff, you'll re-create the state of
        # the _record_ at the present.
        #
        # Unlike the _audit(long, Timestamp)_ method,
        # _diff(long, Timestamp)_ does not necessarily reflect ALL the
        # changes made to _record_ during the time span.
        #
        # @return [Hash]
        # @overload diff(record, start)
        #   List the net changes made to _record_ since _start_. If you begin
        #   with the state of the _record_ at _start_ and re-apply all the
        #   changes in the diff, you'll re-create the state of the same _record_
        #   at the present.
        #   @param [Integer] record The record id
        #   @param [Integer, String] start The base timestamp from which the diff is calculated
        #   @return [Hash] A Hash that associates each key in the _record_ to another Hash that associates a {Diff change description} to the list of values that fit the description (i.e. <code>{"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}</code>)
        # @overload diff(record, start, end)
        #   List the net changes made to _record_ since _start_. If you begin
        #   with the state of the _record_ at _start_ and re-apply all the
        #   changes in the diff, you'll re-create the state of the same _record_
        #   at _end_.
        #   @param [Integer] record The record id
        #   @param [Integer, String] start The base timestamp from which the diff is calculated
        #   @param [Integer, String] end The comparison timestamp to which the diff is calculated
        #   @return [Hash] A Hash that associates each key in the _record_ to another Hash that associates a {Diff change description} to the list of values that fit the description (i.e. <code>{"key": {ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}}</code>)
        # @overload diff(key, record, start)
        #   List the net changes made to _key_ in _record_ since _start_. If you
        #   begin with the state of the field at _start_ and re-apply all the
        #   changes in the diff, you'll re-create the state of the same field at
        #   the present.
        #   @param [String] key The field name
        #   @param [Integer] record The record id
        #   @param [Integer, String] start The base timestamp from which the diff is calculated
        #   @return [Hash] A Hash that associates a {Diff change description} to the list of values that fit the description (i.e. <code>{ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}</code>)
        # @overload diff(key, record, start, end)
        #   List the net changes made to _key_ in _record_ since _start_. If you
        #   begin with the state of the field at _start_ and re-apply all the
        #   changes in the diff, you'll re-create the state of the same field at
        #   _end_.
        #   @param [String] key The field name
        #   @param [Integer] record The record id
        #   @param [Integer, String] start The base timestamp from which the diff is calculated
        #   @param [Integer, String] end The comparison timestamp to which the diff is calculated
        #   @return [Hash] A Hash that associates a {Diff change description} to the list of values that fit the description (i.e. <code>{ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}</code>)
        # @overload diff(key, start)
        #   List the net changes made to _key_ in _record_ since _start_. If you
        #   begin with the state of the field at _start_ and re-apply all the
        #   changes in the diff, you'll re-create the state of the same field at
        #   the present.
        #   @param [String] key The field name
        #   @param [Integer, String] start The base timestamp from which the diff is calculated
        #   @return [Hash] A Hash that associates a {Diff change description} to the list of values that fit the description (i.e. <code>{ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}</code>)
        # @overload diff(key, start, end)
        #   List the net changes made to _key_ in _record_ since _start_. If you
        #   begin with the state of the field at _start_ and re-apply all the
        #   changes in the diff, you'll re-create the state of the same field at
        #   _end_.
        #   @param [String] key The field name
        #   @param [Integer, String] start The base timestamp from which the diff is calculated
        #   @param [Integer, String] end The comparison timestamp to which the diff is calculated
        #   @return [Hash] A Hash that associates a {Diff change description} to the list of values that fit the description (i.e. <code>{ADDED: ["value1", "value2"], REMOVED: ["value3", "value4"]}</code>)
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

        # Terminate the client's session and close this connection.
        # @return [Void]
        def exit
            @client.logout @creds, @environment
            @transport.close
        end

        # Find the records that satisfy the _criteria_.
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

        # Get the most recently added value/s.
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
            return dynamic_dispatch(*args, **kwargs).rubyify
        end

        # Return the unique record where the _key_ equals _value_
        # or throw a DuplicateEntryException If multiple records match the
        # condition. If no record matches, add _key_ as _value_ in a new
        # record and return the id.
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
        #
        # Each of the values in _data_ must be a primitive or one
        # dimensional object (e.g. no nested _associated arrays_ or _multimaps_).
        #
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

        # Return the name of the connected environment.
        # @return [String] the server environment associated with this connection
        def get_server_environment
            return @client.getServerEnvironment @creds, @transaction, @environment
        end

        # Return the version of the connected server.
        #
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

        # Export data as a JSON string.
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

        # Add a link from a field in the _source_ to one or more _destination_
        # records.
        # @return [Boolean, Hash]
        # @overload link(key, destination, source)
        #   Add a link from the _key_ field in the _source_ record to the _destination_ record.
        #   @param [String] key The field name
        #   @param [Integer] destination The record that is the target of the link
        #   @param [Integer] source The record that contains the field to link from
        #   @return [Boolean] A flag that indicates if the link was successfully added from the _key_ field in _source_ to the destination.
        # @overload link(key, destinations, source)
        #   Add a link from the _key_ field in the _source_ record to each of the _destinations_.
        #   @param [String] key The field name
        #   @param [Array] destinations The records that are the target of the link
        #   @param [Integer] source The record that contains the field to link from
        #   @return [Hash] A Hash that maps each of the _destinations_ to a flag that indicates if the link was successfuly added from the _key_ field in _source_ to that destination
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

        # TODO: documentation
        def reconcile(*args, **kwargs)
            key, record, values = args
            key ||= kwargs.fetch(:key, key)
            record ||= kwargs.fetch(:record, record)
            values ||= kwargs.fetch(:values, values)
            values = values.thriftify
            @client.reconcileKeyRecordValues key, record, values, @creds, @transaction, @environment
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

        # Atomically revert data to a previous state.
        # @return [Void]
        # @overload revert(key, record, timestamp)
        #   Revert a field in _record_ to its state at _timestamp_.
        #   @param [String] key The field name
        #   @param [Integer] record The record that contains the field
        #   @param [Integer, String] timestamp The timestamp of the state to restore
        #   @return [Void]
        # @overload revert(keys, record, timestamp)
        #   Revert multiple in fields in _record_ to their state at _timestamp_.
        #   @param [Array] keys The field names
        #   @param [Integer] record The record that contains the field
        #   @param [Integer, String] timestamp The timestamp of the state to restore
        #   @return [Void]
        # @overload revert(key, records, timestamp)
        #   Revert a field in multiple _records_ to their state at _timestamp_.
        #   @param [String] key The field name
        #   @param [Arrays] records The records that contain the field
        #   @param [Integer, String] timestamp The timestamp of the state to restore
        #   @return [Void]
        # @overload revert(keys, records, timestamp)
        #   Revert multiple fields in multiple _records_ to their state at _timestamp_.
        #   @param [Array] keys The field names
        #   @param [Arrays] records The records that contain the fields
        #   @param [Integer, String] timestamp The timestamp of the state to restore
        #   @return [Void]
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
            elsif keys.is_a? String and records.is_a? Array and timestamp and !timestr
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

        # Perform a full text search for _query_ against the _key_
        # field and return the records that contain a _String_ or
        # _Tag_ value that matches.
        #
        # @return [Array] The records that match
        # @overload search(key, query)
        #   Search for all the records that have a value in the _key_ field that fully or partially matches the _query_.
        #   @param [String] key The field name
        #   @param [String] query The search query to match
        #   @return [Array] The records that match
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
            return dynamic_dispatch(*args, **kwargs).rubyify
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

        # Start a new transaction.
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
        # Alternatively, if you supply a block to this method, starting and
        # committing the transactions happens automatically and there is also
        # automatic logic to gracefully handle exceptions that may result from
        # any of the actions in the transaction.
        #
        #   concourse.stage do
        #       concourse.get key:"name", record:1
        #       concourse.add key:"name", value:"Jeff Nelson", record:1
        #   end
        # @return [Void, Boolean]
        def stage(&block)
            if block_given?
                self.stage
                begin
                    block.call
                    self.commit
                rescue TransactionException => e
                    self.abort
                    raise e
                end
            else
                @transaction = @client.stage @creds, @environment
            end
        end

        # Return the server's unix timestamp in microseconds. The precision of
        # the timestamp is subject to network latency.
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

        # Remove the link from a key in _source_ to a _destination_ record.
        # @return [Boolean, Hash]
        # @overload unlink(key, destination, source)
        #   Remove the link from the _key_ field in the _source_ record to the _destination_ record.
        #   @param [String] key The field name
        #   @param [Integer] destination The record that is the target of the link
        #   @param [Integer] source The record that contains the field where the link is from
        #   @return [Boolean] A flag that indicates if the link was successfully removed from the _key_ field in _source_.
        # @overload unline(key, destinations, source)
        #   Remove a link from the _key_ field in the _source_ record to each of the _destinations_.
        #   @param [String] key The field name
        #   @param [Array] destinations The records that are the target of the link
        #   @param [Integer] source The record that contains the field where the link is from
        #   @return [Hash] A Hash that maps each of the _destinations_ to a flag that indicates if the link was successfuly removed from the _key_ field in _source_
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

        # Remove the link from a key in the _source_ to one or more
        # _destination_ records.
        # @return [Boolean] a flag that indicates whether the value exists
        # @overload verify(key, value, record)
        #   Verify that _key_ equals _value_ in _record_.
        #   @param [String] key The field name
        #   @param [Object] value The value to verify
        #   @param [Integer] record The record that contains the field
        #   @return [Boolean] a flag that indicates whether the value exists
        # @overload verify(key, value, record, timestamp)
        #   Verify that _key_ equaled _value_ in _record_ at _timestamp_.
        #   @param [String] key The field name
        #   @param [Object] value The value to verify
        #   @param [Integer] record The record that contains the field
        #   @param [String, Integer] timestamp The timestamp to use for verification
        #   @return [Boolean] a flag that indicates whether the value exists
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

        # Atomically replace _expected_ with _replacement_ for _key_ in
        # _record_ if and only if _expected_ is currently stored in the field.
        #
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

        # Atomically verify that _key_ equals _expected_ in _record_ or set it
        # as such.
        #
        # Please note that after returning, this method guarantees that _key_ in # _record_ will only contain _value_, even if it already existed
        # alongside other values
        # (e.g. calling concourse.verify_or_set("inclusive", "bar", 1) will mean that
        # the field named "inclusive" in record 1 will only have "bar" as a value
        # after returning, even if the field already contained "bar", "baz" and
        # "apple" as values.
        #
        # So, basically, this method has the same guarantee as the [#set] method,
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

        # Authenticate the _username_ and _password_ and populate
        # _creds_ with the appropriate AccessToken.
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

        # Alias for #to_s
        # @!visibility private
        def inspect
            return to_s
        end

        # Dynamically invoke the appropriate thrift method based on the internal
        # caller and the signature made up of the _args_ and _kwargs_.
        # @param [Array] args The positional args
        # @param [Hash] kwargs The keyword arguments
        # @return [Object] The result of the dynamcic function call
        def dynamic_dispatch(*args, **kwargs)
            method = caller[0][/`.*'/][1..-2]
            return @client.send(*(Dispatch.dynamic(method, *args, **kwargs)), @creds, @transaction, @environment)
        end

        private :authenticate, :dynamic_dispatch

    end

end
