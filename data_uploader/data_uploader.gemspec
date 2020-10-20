$:.push File.expand_path("lib", __dir__)

# Maintain your gem's version:
require "data_uploader/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = 'cw_data_uploader'
  s.version     = DataUploader::VERSION
  s.authors     = ['Tiago Santos']
  s.email       = ['tiago.santos@vizzuality.com']
  s.homepage    = 'https://www.climatewatchdata.org'
  s.summary     = 'Climate Watch -> Data Uploader'
  s.description = 'Climate Watch -> Data Uploader'
  s.license     = 'MIT'

  s.files = Dir['{app,config,db,lib}/**/*', 'MIT-LICENSE',
                'Rakefile', 'README.md']

  s.required_ruby_version     = '>= 2.5.1'
  s.required_rubygems_version = '>= 1.8.11'

  # Add activeadmin for simple CMS
  s.add_dependency 'activeadmin'
  s.add_dependency 'devise'

  s.add_dependency 'aws-sdk-rails', '~> 2'
  s.add_dependency 'aws-sdk-s3', '~> 1'

  s.add_dependency 'rubyzip'
  s.add_dependency 'sidekiq', '>= 6.1.0'

  s.add_dependency 'climate_watch_engine', '~> 1.4.0'

  s.add_dependency 'pg'

  s.add_development_dependency 'factory_bot_rails'
  s.add_development_dependency 'rspec-collection_matchers'
  s.add_development_dependency 'rspec-rails'
  s.add_development_dependency 'simplecov'

  s.test_files = Dir['spec/**/*']
end
