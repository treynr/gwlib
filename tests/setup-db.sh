#!/usr/bin/env bash

## file: setup-db.sh
## desc: Sets up the postgres DB and users for testing.
## auth: TR

sudo -u postgres psql -U postgres -c 'CREATE USER test_user;'
sudo -u postgres psql -U postgres -c 'ALTER USER test_user WITH SUPERUSER;'
sudo -u postgres psql -U postgres -c 'CREATE DATABASE test_db;'
sudo -u postgres psql -U postgres test_db -f setup-db.sql
