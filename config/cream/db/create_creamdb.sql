-- User: "cream"

-- DROP USER cream;

-- CREATE USER cream WITH
--  PASSWORD 'cream'
--  CREATEDB CREATEUSER;

-- Database: creamdb

DROP DATABASE creamdb;

CREATE DATABASE creamdb
  WITH OWNER = cream
  ENCODING = 'SQL_ASCII'
  TEMPLATE = template1;


 