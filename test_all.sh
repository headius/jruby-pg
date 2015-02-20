#!/usr/bin/env bash

# This script will run the test suite with and without SSL

export JRUBY_OPTS="--dev"
bundle exec rake clean
bundle exec rake                            # test without ssl
PGSSLMODE='require' bundle exec rake        # test with ssl
bundle exec rake package

rake gem && \
    pg_pkg=$(readlink -f $(ls -tr pkg/*.gem | head)) && \
    pg_ver=$(ruby -Ilib -rpg -e 'puts PG::VERSION')

if [ ! $? ]; then
    echo "Cannot package the pg gem, fix the errors and try again"
    exit 1
fi

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
git checkout v3.2.21

# replace the jdbc gem with pg and force rails to use us instead
grep -E -v 'pg' Gemfile > $TMPDIR/gemfile.1
sed -E "s/gem 'activerecord-jdbcpostgresql.*/gem 'pg', '${pg_ver}'/g" $TMPDIR/gemfile.1 > $TMPDIR/gemfile.2
grep -E -v "jdbc|racc" $TMPDIR/gemfile.2 > Gemfile

# finish the setup and start running the tests
source ~/.rvm/scripts/rvm
rvm use --create jruby@rails-test-jruby-pg
gem uninstall pg -I -a -x
gem install $pg_pkg
gem install --version 1.7.7 json_pure
gem install --version 1.7.7 json
gem install bundler
bundle update
pushd activerecord
PATH=$PATH:$(pg_config --bindir)
bundle exec rake postgresql:rebuild_databases
# A reminder: If you want to run a single file, use the following
# rake postgresql:test TEST='test/cases/migration/column_attributes_test.rb'
bundle exec rake test_postgresql | tee $TMPDIR/rails.test.output
popd
popd
