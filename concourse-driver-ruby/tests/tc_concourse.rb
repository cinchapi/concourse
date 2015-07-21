require 'test/unit'
require 'socket'
require_relative '../lib/concourse'

class RubyClientDriverTest < Test::Unit::TestCase

    @@client = nil

    def initialize(arg)
        super(arg)
        path = File.expand_path('../../../mockcourse/mockcourse', __FILE__)
        port = get_open_port
        path = "#{path} #{port} > /dev/null 2>&1 &"
        @@pid = Process.spawn(path, :pgroup => true)
        tries = 5
        while tries > 0 and @@client.nil?
            tries -= 1
            sleep(1) # wait for Mockcourse to start
            begin
                @@client = Concourse.new(port:port)
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
    end

    def self.finalize(pid)
        proc { exec("kill -9 #{pid}") }
    end

    def teardown
        @@client.logout
    end

    def test_add_key_value_record
        key = "foo"
        value = "static value"
        record = 17
        assert(@@client.add key:key, value:value, record:record)
    end

    def get_open_port
        socket = Socket.new(:INET, :STREAM, 0)
        socket.bind(Addrinfo.tcp("127.0.0.1", 0))
        port =  socket.local_address.ip_port
        socket.close
        port
    end

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
