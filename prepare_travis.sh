#!/usr/bin/env bash

who=`whoami`

pg_conf=$(psql -t -c 'show hba_file;' | tr -d ' \t\n')

sudo -u postgres createuser -s $who
echo "host    all             $who        ::1/128            trust" | sudo tee $pg_conf
