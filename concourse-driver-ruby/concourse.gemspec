Gem::Specification.new do |s|
    s.name  = 'concourse-driver-ruby'
    s.version = '0.5.0'
    s.authors = ['Cinchapi Inc.']
    s.email = 'concourse-devs@googlegroups.com'
    s.homepage = 'http://concoursedb.com'
    s.summary = 'Ruby driver for Concourse!'
    s.description = 'Ruby driver for Concourse'
    s.license = 'Apache License Version 2.0'
    s.require_paths = ['lib']
    s.files = Dir.glob('{lib}/**/*')
    s.files += ['LICENSE.txt']

    s.add_dependency('thrift', '~> 0.9.3')
    s.add_dependency('java-properties', '~> 0.0.2')
    s.add_dependency('connection_pool', '~> 2.2.0')
end
