lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

# Maintain your gem's version:
require 'rocket_job/version'

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = 'rocket_job'
  s.version     = RocketJob::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ['Reid Morrison']
  s.email       = ['reidmo@gmail.com']
  s.homepage    = 'https://github.com/reidmorrison/rocket_job'
  s.summary     = "High volume, priority based, Enterprise Batch Processing solution for Ruby"
  s.description = "Designed for batch processing from single records to millions of records in a single batch. Uses threading instead of process forking for greater throughtput."
  s.executables = ['rocket_job']
  s.files       = Dir["lib/**/*", "bin/*", "LICENSE.txt", "Rakefile", "README.md"]
  s.test_files  = Dir["test/**/*"]
  s.license     = "Apache License V2.0"
  s.has_rdoc    = true
  s.add_dependency 'aasm', '~> 4.1'
  s.add_dependency 'semantic_logger', '~> 2.13'
  s.add_dependency 'mongo_ha', '~> 1.11'
  s.add_dependency 'mongo', '~> 1.11'
  s.add_dependency 'mongo_mapper', '~> 0.13'
  s.add_dependency 'symmetric-encryption', '~> 3.0'
  s.add_dependency 'sync_attr', '~> 2.0'
end
