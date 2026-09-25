-- pg_rest version of pg_net's test/init.sql
create database pre_existing;
create role pre_existing nosuperuser login;

\c postgres
create extension pg_rest;
\ir ./utils/loadtest_rest.sql
