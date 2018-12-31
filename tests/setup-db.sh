#!/usr/bin/env bash

## file: setup-db.sh
## desc: Sets up the postgres DB and users for testing.
## auth: TR

psql -U postgres -p 5433 -c 'CREATE USER test_user;'
psql -U postgres -p 5433 -c 'ALTER USER test_user WITH SUPERUSER;'
psql -U postgres -p 5433 -c 'CREATE DATABASE test_db;'
psql -U postgres -p 5433 test_db -f setup-db.sql
