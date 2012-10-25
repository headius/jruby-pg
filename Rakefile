# -*- ruby -*-

require 'rubygems'
require 'hoe'

Hoe.plugin :gemspec
Hoe.plugin :bundler
Hoe.plugin :test

HOE = Hoe.spec 'pg' do
  self.readme_file  = ['README',    ENV['HLANG'], 'rdoc'].compact.join('.')
  self.history_file = ['CHANGELOG', ENV['HLANG'], 'rdoc'].compact.join('.')

  developer('Charles Nutter', 'headius@headius.com')
  developer('John Shahid', 'jvshahid@gmail.com')

  self.extra_dev_deps += [
                          ["hoe-bundler",     ">= 1.1"],
                          ["hoe-gemspec",     ">= 1.0"],
                          ["rake",            ">= 0.9"],
                          ["rake-compiler",   "=  0.8.0"],
                          ["rspec"]
                         ]

  self.spec_extras = { :platform => 'java' }
end

require "rake/javaextensiontask"
Rake::JavaExtensionTask.new("pg", HOE.spec) do |ext|
  jruby_home = RbConfig::CONFIG['prefix']
  ext.ext_dir = 'ext/java'
  ext.lib_dir = 'lib/pg'
  jars = ["#{jruby_home}/lib/jruby.jar"] + FileList['lib/*.jar']
  ext.classpath = jars.map { |x| File.expand_path x }.join ':'
end

gem_build_path = File.join 'pkg', HOE.spec.full_name

task gem_build_path => [:compile] do
  cp 'lib/pg/pg.jar', File.join(gem_build_path, 'lib', 'pg')
  HOE.spec.files += ['lib/pg/pg.jar']
end

# vim: syntax=ruby
