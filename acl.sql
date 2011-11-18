--
-- acl.sql - permissions
-- Part of an implementation of an 'ordered' data type.
--
-- This file should normally be run from ordered.sql, but can be run
-- alone if necessary.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

SET search_path TO ordered1;

GRANT USAGE ON SCHEMA ordered1 TO PUBLIC;
GRANT EXECUTE 
    ON FUNCTION 
        -- tree.sql
        _verify_att(pg_attribute),
        create_ordering(name, name),
        _set_ordering_for(oid, oid, int2),

        -- value.sql
        --before(ordered),
        --after(ordered),

        -- op.sql
        ancestors(ordered),
        ordered_cmp(ordered, ordered),
        ordered_lt(ordered, ordered),
        ordered_le(ordered, ordered),
        ordered_ge(ordered, ordered),
        ordered_gt(ordered, ordered),
        ordered_eq(ordered, ordered),
        ordered_ne(ordered, ordered)
    TO PUBLIC;

