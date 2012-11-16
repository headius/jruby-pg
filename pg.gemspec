# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "pg"
  s.version = "0.14.1.20121112164003"
  s.platform = "java"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Charles Nutter", "John Shahid"]
  s.date = "2012-11-12"
  s.description = "A Java implementation of the pg gem."
  s.email = ["headius@headius.com", "jvshahid@gmail.com"]
  s.executables = ["pg"]
  s.extra_rdoc_files = ["CHANGELOG.rdoc", "Manifest.txt", "README.rdoc"]
  s.files = ["CHANGELOG.rdoc", "Manifest.txt", "README.rdoc", "Rakefile", "bin/pg", "lib/pg.rb", "lib/pg/result.rb", "lib/pg/constants.rb", "lib/pg/connection.rb", "lib/pg/exceptions.rb", "lib/postgresql-9.1-903.jdbc4.jar", "lib/pg/pg.jar", ".gemtest"]
  s.homepage = "https://github.com/headius/jruby-pg"
  s.rdoc_options = ["--main", "README.rdoc"]
  s.require_paths = ["lib"]
  s.rubyforge_project = "pg"
  s.rubygems_version = "1.8.24"
  s.summary = "A Java implementation of the pg gem."

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rdoc>, ["~> 3.10"])
      s.add_development_dependency(%q<hoe-bundler>, [">= 1.1"])
      s.add_development_dependency(%q<hoe-gemspec>, [">= 1.0"])
      s.add_development_dependency(%q<rake>, [">= 0.9"])
      s.add_development_dependency(%q<rake-compiler>, ["= 0.8.0"])
      s.add_development_dependency(%q<git>, [">= 0"])
      s.add_development_dependency(%q<logger>, [">= 0"])
      s.add_development_dependency(%q<rspec>, [">= 0"])
      s.add_development_dependency(%q<hoe>, ["~> 3.1"])
    else
      s.add_dependency(%q<rdoc>, ["~> 3.10"])
      s.add_dependency(%q<hoe-bundler>, [">= 1.1"])
      s.add_dependency(%q<hoe-gemspec>, [">= 1.0"])
      s.add_dependency(%q<rake>, [">= 0.9"])
      s.add_dependency(%q<rake-compiler>, ["= 0.8.0"])
      s.add_dependency(%q<git>, [">= 0"])
      s.add_dependency(%q<logger>, [">= 0"])
      s.add_dependency(%q<rspec>, [">= 0"])
      s.add_dependency(%q<hoe>, ["~> 3.1"])
    end
  else
    s.add_dependency(%q<rdoc>, ["~> 3.10"])
    s.add_dependency(%q<hoe-bundler>, [">= 1.1"])
    s.add_dependency(%q<hoe-gemspec>, [">= 1.0"])
    s.add_dependency(%q<rake>, [">= 0.9"])
    s.add_dependency(%q<rake-compiler>, ["= 0.8.0"])
    s.add_dependency(%q<git>, [">= 0"])
    s.add_dependency(%q<logger>, [">= 0"])
    s.add_dependency(%q<rspec>, [">= 0"])
    s.add_dependency(%q<hoe>, ["~> 3.1"])
  end
end
