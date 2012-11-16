# -*- ruby -*-

require 'rubygems'
require 'hoe'
require 'git'
require 'logger'

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
                          ["git"],
                          ["logger"],
                          ["rspec"]
                         ]

  self.spec_extras = { :platform => 'java' }

  self.clean_globs = ['spec/*', 'tmp_test_specs', 'tmp']
end

require "rake/javaextensiontask"
Rake::JavaExtensionTask.new("pg_ext", HOE.spec) do |ext|
  jruby_home = RbConfig::CONFIG['prefix']
  ext.ext_dir = 'ext/java'
  ext.lib_dir = 'lib'
  jars = ["#{jruby_home}/lib/jruby.jar"] + FileList['lib/*.jar']
  ext.classpath = jars.map { |x| File.expand_path x }.join ':'
end

gem_build_path = File.join 'pkg', HOE.spec.full_name

task gem_build_path => [:compile] do
  cp 'lib/pg_ext.jar', File.join(gem_build_path, 'lib')
  HOE.spec.files += ['lib/pg_ext.jar']
end

def remote
  "git://github.com/jvshahid/ruby-pg.git"
end

desc 'fetch the specs from the ruby-pg repo'
task 'get-ruby-pg-specs' do
  if Dir.glob('spec/*').empty?
    FileUtils.rm_rf '/tmp/checkout'
    g = Git.clone(remote, '/tmp/checkout', :log => Logger.new(STDOUT))
    g.checkout('fix_path_to_pg_binaries')
    FileUtils.cp_r Dir.glob('/tmp/checkout/spec/*'), 'spec/'
  end
end

desc "make sure the spec directory has the latest ruby-pg and jruby-pg specs"
task :'sync-files' => 'get-ruby-pg-specs'

Rake::Task[:spec].prerequisites << 'sync-files'
Rake::Task[:spec].prerequisites << :compile
Rake::Task[:spec].prerequisites << :java_debug

# sync specs from jruby-spec to spec/jruby
target_dir = 'spec/jruby'
directory target_dir
Dir.chdir 'jruby-spec' do
  Dir.glob('**/*').each do |f|
    file_name = "jruby-spec/#{f}"
    new_name = "#{target_dir}/#{f}"
    t = file new_name => [file_name, target_dir] do |t|
      if File.directory? file_name
        FileUtils.mkpath new_name
      else
        FileUtils.cp file_name, new_name
        File.chmod 0444, new_name
      end
    end
    Rake::Task[:'sync-files'].prerequisites << t
  end
end
task :java_debug do
  ENV['JAVA_OPTS'] = '-Xdebug -Xrunjdwp:transport=dt_socket,address=8080,server=y,suspend=n' if ENV['JAVA_DEBUG'] == '1'
end
# vim: syntax=ruby
