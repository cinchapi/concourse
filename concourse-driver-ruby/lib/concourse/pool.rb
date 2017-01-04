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

require 'concourse/client'
require 'connection_pool'

module Concourse

    # This is an alias for the Concourse::Pool constructor
    # @return [Concourse::Pool] the connection pool
    def self.connection_pool(host: "localhost", port: 1717, username: "admin", password: "admin", environment: "", **kwargs)
        return Concourse::Pool.new(host: host, port: port, username: username, password: password, environment: environment, **kwargs)
    end

    # A pool of connections to share amongst the fibers or threads in a Ruby
    # application. The pool lazily creates connections to Concourse, up to a
    # configurable maximum.
    #
    # Usage:
    #
    #   pool = Concourse.connection_pool
    #   pool.request do |conn|
    #       puts conn.get_server_version
    #   end
    #
    class Pool < ConnectionPool

        # The default timeout.
        @@default_timeout = 5

        # The default limit for the number of simultaneous connections that can
        # be leased from the pool.
        @@default_size = 5

        # Initialize a new connection pool.
        # @param host [String] the server host
        # @param port [Integer] the listener port
        # @param username [String] the username with which to connect
        # @param password [String] the password for the username
        # @param environment [String] the environment to use, by default the default_environment` in the server's concourse.prefs file is used
        # @option kwargs [String] :prefs  You may specify the path to a preferences file using the 'prefs' keyword argument. If a prefs file is supplied, the values contained therewithin for any of the arguments above become the default if those arguments are not explicitly given values.
        # @option kwargs [Integer] :timeout The number of seconds to block before issuing a timeout when waiting for a connection to become available.
        # @option kwargs [Integer] :size The maximum number of simultaneous connections to lease from the pool. Once this limit is reached, all subsequent requests will block until one of the connections becomes available again.
        #
        # @return [Pool] The connection pool
        def initialize(host = "localhost", port = 1717, username = "admin", password = "admin", environment = "", **kwargs)
            size = kwargs.fetch(:size, @@default_size)
            timeout = kwargs.fetch(:timeout, @@default_timeout)
            super(size:size, timeout:timeout) do
                Concourse.connect(host:host, port:port, username:username, password:password, environment:environment, **kwargs)
            end
        end

        # Shutdown the pool.
        def shutdown
            super() do |conn|
                conn.close
            end
        end

        # Request a connection from the pool, and block up to the defined
        # timeout if necessary.
        def request(&block)
            self.with(&block)
        end

    end
end
