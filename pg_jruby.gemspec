# -*- encoding: utf-8 -*-
# stub: pg_jruby 0.14.1.rc1.20131117185043 java lib

Gem::Specification.new do |s|
  s.name = "pg_jruby"
  s.version = "0.14.1.rc1.20131117185043"
  s.platform = "java"

  s.required_rubygems_version = Gem::Requirement.new("> 1.3.1") if s.respond_to? :required_rubygems_version=
  s.authors = ["Charles Nutter", "John Shahid"]
  s.date = "2013-11-17"
  s.description = "This is a native implementation of the Postsgres protocol written\nentirely in Java. The project was started by @headius as a ruby-pg\nreplacement for JRuby that uses the JDBC driver and private API of\nPostgres. Unfortunately ruby-pg (which uses libpq under the hood)\nexposed a lot of features of Postgres that were impossible to\nimplement or were complicated given the nature of the JDBC and the\nencapsulation of many features that are exposed in ruby-pg.\n\n*WARNING* this gem is not production ready yet. There are many\nbugs that needs to be fixed and more testing. So please checkout\nthe code and submit pull requests with some fixes. If your Java-Fu\nisn't that great you can still contribute by submitting test cases\nand We'll be happy to fix them."
  s.email = ["headius@headius.com", "jvshahid@gmail.com"]
  s.executables = ["pg"]
  s.extra_rdoc_files = ["CHANGELOG.rdoc", "Manifest.txt", "README.rdoc"]
  s.files = ["CHANGELOG.rdoc", "Manifest.txt", "README.rdoc", "Rakefile", "bin/pg", "lib/pg.rb", "lib/pg/result.rb", "lib/pg/constants.rb", "lib/pg/connection.rb", "lib/pg/exceptions.rb", "lib/pg_ext.jar", ".gemtest"]
  s.homepage = "https://github.com/headius/jruby-pg"
  s.licenses = ["MIT"]
  s.rdoc_options = ["--main", "README.rdoc"]
  s.require_paths = ["lib"]
  s.rubyforge_project = "pg_jruby"
  s.rubygems_version = "2.1.9"
  s.summary = "This is a native implementation of the Postsgres protocol written entirely in Java"

  if s.respond_to? :specification_version then
    s.specification_version = 4

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
