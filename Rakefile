lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'rubygems'
require 'rake/clean'
require 'rake/testtask'
require 'date'
require 'skynet/version'

desc "Build gem"
task :gem  do |t|
  gemspec = Gem::Specification.new do |spec|
    spec.name        = 'skynet'
    spec.version     = ResilientSocket::VERSION
    spec.platform    = Gem::Platform::RUBY
    spec.authors     = ['Reid Morrison']
    spec.email       = ['reidmo@gmail.com']
    spec.homepage    = 'https://github.com/ClarityServices/ruby_skynet'
    spec.date        = Date.today.to_s
    spec.summary     = "Ruby Client and Server into Skynet"
    spec.description = "Ruby Client and Server into Skynet"
    spec.files       = FileList["./**/*"].exclude('*.gem', 'nbproject').map{|f| f.sub(/^\.\//, '')}
    spec.has_rdoc    = true
    spec.add_dependency 'semantic_logger'
    spec.add_dependency 'resilient_socket'
    spec.add_dependency 'gene_pool'
    spec.add_dependency 'fraggle'
    spec.add_dependency 'bson'
  end
  Gem::Builder.new(gemspec).build
end

desc "Run Test Suite"
task :test do
  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/*_test.rb']
    t.verbose    = true
  end

  Rake::Task['functional'].invoke
end
