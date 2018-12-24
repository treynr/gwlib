#!/usr/bin/env bash

## file: setup-db.sh
## desc: Sets up the postgres DB and users for testing.
## auth: TR

psql -U postres -c 'CREATE USER test_user;'
psql -U postgres -c 'ALTER USER test_user WITH SUPERUSER;'
psql -U postgres -c 'CREATE DATABASE test_db;'
