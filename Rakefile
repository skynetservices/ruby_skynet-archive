lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require 'rake/clean'
require 'rake/testtask'
require 'semantic_logger'
require 'ruby_skynet/version'

desc "Run Test Suite"
task :test do
  Rake::TestTask.new(:functional) do |t|
    t.test_files = FileList['test/*_test.rb']
    t.verbose    = true
  end

  Rake::Task['functional'].invoke
end

task :gem do
  system "gem build ruby_skynet.gemspec"
end

task :release => :gem do
  system "git tag -a v#{RubySkynet::VERSION} -m 'Tagging #{RubySkynet::VERSION}'"
  system "git push --tags"
  system "gem push ruby_skynet-#{RubySkynet::VERSION}.gem"
  system "rm ruby_skynet-#{RubySkynet::VERSION}.gem"
end

