# Copyright (c) 2015 Cinchapi, Inc.
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

require 'concourse'
require 'socket'
require 'test/unit'

class IntegrationBaseTest < Test::Unit::TestCase

    @@client = nil
    @@has_setup = false

    # Initialize each integrate test class by starting mockcourse and
    # connecting
    def initialize(arg)
        super(arg)
        if(!@@has_setup)
            path = File.expand_path('../../../mockcourse/mockcourse', __FILE__)
            port = get_open_port
            path = "#{path} #{port} > /dev/null 2>&1 &"
            @@pid = Process.spawn(path, :pgroup => true)
            tries = 5
            while tries > 0 and @@client.nil?
                tries -= 1
                sleep(1) # wait for Mockcourse to start
                begin
                    @@client = Concourse.connect(port:port)
                rescue Exception => ex
                    if tries == 0
                        raise ex
                    else
                        next
                    end
                end
            end
            @@pid = get_mockcourse_pid
            ObjectSpace.define_finalizer(self, self.class.finalize(@@pid))
            @@has_setup = true
        end
    end

    # Finalizer that will kill Mockcourse after all the unit tests have run
    def self.finalize(pid)
        proc { exec("kill -9 #{pid}") }
    end

    # Run setup before each test
    def setup
        @client = @@client
    end

    # Run teardown after each test
    def teardown
        @client.logout
    end

    # Return an open port that is chosen by the OS
    # @return [Integer] the port
    def get_open_port
        socket = Socket.new(:INET, :STREAM, 0)
        socket.bind(Addrinfo.tcp("127.0.0.1", 0))
        port =  socket.local_address.ip_port
        socket.close
        port
    end

     # Ruby seemingly does not have a good way to setsid and exec/fork a
     # background process whilist keeping up with the parent process id so
     # that we can kill it upon termination and stop Mockcourse. To get around
     # that we have to store the PID of the bash script that launches the
     # Groovy process for Mockcourse and then query for all the groovy
     # processes and get the PID that is closes to the one we stored and assume
     # that is the PID of the Mockcourse groovy process. Killing that process
     # will kill all Mockcourse related processes that we indeed started
     # @return [Integer] the process id that we want to kill
    def get_mockcourse_pid
        script = File.expand_path('../../../mockcourse/getpid', __FILE__)
        out = `#{script}`
        pids = out.lines.map(&:chomp)
        pid = 0
        _delta = 0
        for x in pids
            delta = (x.to_i - @@pid).abs
            if pid == 0 or (pid != 0 and delta < _delta)
                _delta = delta
                pid = x
            end
        end
        pid
    end

    private :get_open_port, :get_mockcourse_pid

end
