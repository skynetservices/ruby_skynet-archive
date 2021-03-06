lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'date'
require 'ruby_skynet/version'

Gem::Specification.new do |spec|
  spec.name        = 'ruby_skynet'
  spec.version     = RubySkynet::VERSION
  spec.platform    = Gem::Platform::RUBY
  spec.authors     = ['Reid Morrison']
  spec.email       = ['reidmo@gmail.com']
  spec.homepage    = 'https://github.com/ClarityServices/ruby_skynet'
  spec.date        = Date.today.to_s
  spec.summary     = "Skynet Ruby Client"
  spec.description = "Ruby Client for invoking Skynet services"
  spec.files       = Dir.glob("lib/**/*") + Dir.glob("examples/**/*.rb") +  %w(LICENSE.txt README.md)
  spec.license     = "Apache License V2.0"
  spec.has_rdoc    = true
  spec.add_dependency 'semantic_logger', '>= 2.6.1'
  spec.add_dependency 'resilient_socket', '>= 0.5.0'
  spec.add_dependency 'sync_attr', '>= 1.0.0'
  spec.add_dependency 'bson', '>= 2.0.0.rc3'
  spec.add_dependency 'thread_safe'
end
