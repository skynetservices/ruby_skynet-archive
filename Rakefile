lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'rubygems'
require 'rubygems/package'
require 'rake/clean'
require 'rake/testtask'
require 'date'
require 'semantic_logger'
require 'ruby_skynet/version'

desc "Build gem"
task :gem  do |t|
  gemspec = Gem::Specification.new do |spec|
    spec.name        = 'ruby_skynet'
    spec.version     = RubySkynet::VERSION
    spec.platform    = Gem::Platform::RUBY
    spec.authors     = ['Reid Morrison']
    spec.email       = ['reidmo@gmail.com']
    spec.homepage    = 'https://github.com/ClarityServices/ruby_skynet'
    spec.date        = Date.today.to_s
    spec.summary     = "Skynet Ruby Client"
    spec.description = "Ruby Client for invoking Skynet services"
    spec.files       = FileList["./**/*"].exclude(/\.gem$/, /\.log$/,/nbproject/).map{|f| f.sub(/^\.\//, '')}
    spec.license     = "Apache License V2.0"
    spec.has_rdoc    = true
    spec.add_dependency 'semantic_logger'
    spec.add_dependency 'resilient_socket'
    spec.add_dependency 'multi_json', '>= 1.6.1'
    spec.add_dependency 'bson'
    spec.add_dependency 'ruby_protobuf'
    spec.add_dependency 'celluloid-io', '>= 0.13.0.pre2'
  end
  Gem::Package.build gemspec
end

desc "Run Test Suite"
task :test do
  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/*_test.rb']
    t.verbose    = true
  end

  Rake::Task['functional'].invoke
end
