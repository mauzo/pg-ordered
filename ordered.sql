--
-- ordered.sql - main install entry point
-- An implementation of an 'ordered' data type, which keeps its values
-- sorted in an arbitrary order.
--
-- Run this file through psql(1), as a database superuser, to install
-- the data type and support functions.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

BEGIN;

-- Schema

DROP SCHEMA IF EXISTS ordered1 CASCADE;
CREATE SCHEMA ordered1;
SET search_path TO ordered1;

-- Data types

CREATE TYPE ordered AS (
    rel     oid,
    id      integer
);

-- Include the other sql files

\i util.sql
\i tree.sql
\i op.sql
\i value.sql

\i acl.sql

COMMIT;
