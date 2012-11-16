#!/usr/bin/env bash

# This script will run the test suite with and without SSL

rake                            # test without ssl
PG_TEST_SLL=1 rake              # test with ssl
