#!/usr/bin/env bash

# This script will run the test suite with and without SSL

# rake                            # test without ssl
# PG_TEST_SLL=1 rake              # test with ssl

rake package
pg_pkg=$(readlink -f $(ls -tr pkg/*.gem | head))
pg_ver=$(ruby -Ilib -rpg -e 'puts PG::VERSION')
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

grep -E -v 'pg' Gemfile > $TMPDIR/gemfile_wo_ruby_pg
sed -E "s/gem 'activerecord-jdbcpostgresql.*/gem 'pg', '${pg_ver}'/g" $TMPDIR/gemfile_wo_ruby_pg > $TMPDIR/gemfile_with_jruby_pg
grep -E -v "jdbc" $TMPDIR/gemfile_with_jruby_pg > Gemfile
source ~/.rvm/scripts/rvm
rvm use --create jruby@rails-test-jruby-pg
gem uninstall pg -a -x
gem install $pg_pkg
gem install bundler
bundle install
pushd activerecord
PATH=$PATH:$(pg_config --bindir)
bundle exec rake postgresql:rebuild_databases
bundle exec rake postgresql:test
popd
popd
