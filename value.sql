--
-- value.sql - functions for manipulating ordered values
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

--CREATE FUNCTION after (ordered)
--    RETURNS ordered
--    LANGUAGE sql
--    AS $$
--        SELECT ROW(($1).rel, ($1).ix || B'1')::ordered;
--    $$;
--
--CREATE FUNCTION before (ordered)
--    RETURNS ordered
--    SET search_path FROM CURRENT
--    LANGUAGE sql
--    AS $$
--        SELECT ROW(($1).rel, ($1).ix || B'0')::ordered;
--    $$;
