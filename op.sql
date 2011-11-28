--
-- op.sql - indexable operators
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

-- These functions all need to be VOLATILE, otherwise they don't see
-- records in the ordering table that have been inserted since the
-- beginning of the statement. This means direct comparisons with the
-- results of before() and after() (including the comparisons performed
-- by an index) return NULL, which is Bad.

CREATE FUNCTION ancestors (o ordered)
    RETURNS TABLE ( id integer, depth integer, cmp integer )
    VOLATILE STRICT
    SET search_path FROM CURRENT
    -- The planner seems to plan this recursive with rather badly. The
    -- query ends up doing lots of joins, each of which joins in one
    -- other record, so it should use indexes; but the planner assumes
    -- each join will join in lots of records so it uses seq scans
    -- instead.
    SET enable_seqscan TO off
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            q text;
        BEGIN
            q := replace($an$
                WITH RECURSIVE
                    child ( id, parent, first, depth, cmp ) 
                    AS (
                        SELECT
                            id, parent, first, 0, 0
                            FROM $ord$
                            WHERE id = $1
                        UNION ALL
                        SELECT
                            p.id, p.parent, p.first, c.depth + 1,
                            CASE WHEN c.first THEN -1 ELSE 1 END
                        FROM child c
                            JOIN $ord$ p
                            ON p.id = c.parent
                    )
                SELECT id, depth, cmp FROM child
            $an$, '$ord$', (o).rel::regclass::text);
            --RAISE NOTICE 'SQL: %', q;
            RETURN QUERY EXECUTE q USING o.id;

            RETURN;
        END;
    $fn$;

CREATE FUNCTION ordered_cmp (a ordered, b ordered)
    RETURNS integer
    VOLATILE STRICT
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            rv integer;
        BEGIN
            IF (a).rel <> (b).rel THEN
                RAISE 'can''t compare ordered values from different orderings';
            END IF;

            IF (a).id = (b).id THEN
                RETURN 0;
            END IF;

            SELECT 
                btint4cmp(aa.cmp, ba.cmp)
                FROM ancestors(a) aa
                    JOIN ancestors(b) ba
                    ON aa.id = ba.id
                ORDER BY aa.depth
                LIMIT 1
                INTO rv;

            RETURN rv;
        END;
    $fn$;

-- Comparison functions, in terms of ordered_cmp

CREATE FUNCTION ordered_lt (ordered, ordered)
    RETURNS boolean
    VOLATILE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) < 0 $$;

CREATE FUNCTION ordered_le (ordered, ordered)
    RETURNS boolean
    VOLATILE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) <= 0 $$;

CREATE FUNCTION ordered_eq (ordered, ordered)
    RETURNS boolean
    VOLATILE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) = 0 $$;

CREATE FUNCTION ordered_ne (ordered, ordered)
    RETURNS boolean
    VOLATILE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) <> 0 $$;

CREATE FUNCTION ordered_ge (ordered, ordered)
    RETURNS boolean
    VOLATILE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) >= 0 $$;

CREATE FUNCTION ordered_gt (ordered, ordered)
    RETURNS boolean
    VOLATILE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) > 0 $$;

-- Operators

CREATE OPERATOR < (
    leftarg     = ordered,
    rightarg    = ordered,
    procedure   = ordered_lt,
    commutator  = >,
    negator     = >=,
    restrict    = scalarltsel,
    join        = scalarltjoinsel
);

CREATE OPERATOR <= (
    leftarg     = ordered,
    rightarg    = ordered,
    procedure   = ordered_le,
    commutator  = >=,
    negator     = >,
    restrict    = scalarltsel,
    join        = scalarltjoinsel
);

CREATE OPERATOR > (
    leftarg     = ordered,
    rightarg    = ordered,
    procedure   = ordered_gt,
    commutator  = <,
    negator     = <=,
    restrict    = scalargtsel,
    join        = scalargtjoinsel
);

CREATE OPERATOR >= (
    leftarg     = ordered,
    rightarg    = ordered,
    procedure   = ordered_ge,
    commutator  = <=,
    negator     = <,
    restrict    = scalargtsel,
    join        = scalargtjoinsel
);

CREATE OPERATOR = (
    leftarg     = ordered,
    rightarg    = ordered,
    procedure   = ordered_eq,
    commutator  = =,
    negator     = <>,
    restrict    = eqsel,
    join        = eqjoinsel
);

CREATE OPERATOR <> (
    leftarg     = ordered,
    rightarg    = ordered,
    procedure   = ordered_ne,
    commutator  = <>,
    negator     = =,
    restrict    = neqsel,
    join        = neqjoinsel
);

CREATE OPERATOR CLASS ordered_ops
    DEFAULT FOR TYPE ordered USING btree AS
        OPERATOR    1   <,
        OPERATOR    2   <=,
        OPERATOR    3   =,
        OPERATOR    4   >=,
        OPERATOR    5   >,
        FUNCTION    1   ordered_cmp(ordered, ordered);

