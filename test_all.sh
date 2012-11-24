#!/usr/bin/env bash

# This script will run the test suite with and without SSL

rake                            # test without ssl
PG_TEST_SLL=1 rake              # test with ssl

rake package && \
    pg_pkg=$(readlink -f $(ls -tr pkg/*.gem | head)) && \
    pg_ver=$(ruby -Ilib -rpg -e 'puts PG::VERSION')

if [ ! $? ]; then
    echo "Cannot package the pg gem, fix the errors and try again"
    exit 1
fi

# clone my json branch that has a fix for the merge problem
json_dir=tmp_json
if [ ! -d $json_dir ]; then
    if ! git clone git://github.com/jvshahid/json.git $json_dir ; then
        echo "Cannot clone json from ${json_dir}"
        exit 1
    fi
fi
pushd $json_dir
git checkout fix_merge
rvm use --create jruby@json
bundle install
rake jruby_gem
rake package
popd
echo "$json_dir/pkg"
ls $json_dir/pkg
json_gem=$(readlink -f $json_dir/pkg/json-1.7.5-java.gem)
json_pure_gem=$(readlink -f $json_dir/pkg/json_pure-1.7.5.gem)

# Run rails test suite
rails_dir=tmp_rails
if [ ! -d $rails_dir ]; then
    if ! git clone https://github.com/rails/rails.git $rails_dir ; then
        echo "Cannot clone the rails repo"
        exit 1
    fi
fi

if [ -z "$TMPDIR" ]; then
    TMPDIR=/tmp
fi

pushd $rails_dir

# reset any changes and get the latest
git checkout .
git fetch --all && git rebase origin/master

# replace the jdbc gem with pg and force rails to use us instead
grep -E -v 'pg' Gemfile > $TMPDIR/gemfile_wo_ruby_pg
sed -E "s/gem 'activerecord-jdbcpostgresql.*/gem 'pg', '${pg_ver}'/g" $TMPDIR/gemfile_wo_ruby_pg > $TMPDIR/gemfile_with_jruby_pg
grep -E -v "jdbc" $TMPDIR/gemfile_with_jruby_pg > Gemfile

# finish the setup and start running the tests
source ~/.rvm/scripts/rvm
rvm use --create jruby@rails-test-jruby-pg
gem uninstall pg -a -x
gem install $pg_pkg
gem install $json_gem
gem install $json_pure_gem
gem install bundler
bundle install
pushd activerecord
PATH=$PATH:$(pg_config --bindir)
bundle exec rake postgresql:rebuild_databases
# A reminder: If you want to run a single file, use the following
# rake postgresql:test TEST='test/cases/migration/column_attributes_test.rb'
bundle exec rake postgresql:test
popd
popd
