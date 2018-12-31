#!/usr/bin/env bash

## file: setup-db.sh
## desc: Sets up the postgres DB and users for testing.
## auth: TR

psql -c 'CREATE USER test_user;'
psql -c 'ALTER USER test_user WITH SUPERUSER;'
psql -c 'CREATE DATABASE test_db;'
psql test_db -f setup-db.sql
