BEGIN;

DROP SCHEMA IF EXISTS ordered1 CASCADE;
CREATE SCHEMA ordered1;
SET search_path TO ordered1;

CREATE TYPE ordered AS (
    rel     oid,
    ix      varbit
);

CREATE FUNCTION after (ordered)
    RETURNS ordered
    LANGUAGE sql
    AS $$
        SELECT ROW(($1).rel, ($1).ix || B'1')::ordered;
    $$;

CREATE FUNCTION before (ordered)
    RETURNS ordered
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$
        SELECT ROW(($1).rel, ($1).ix || B'0')::ordered;
    $$;

CREATE FUNCTION ordered_cmp (ordered, ordered)
    RETURNS integer
    IMMUTABLE STRICT
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $$
        -- It should be possible to optimize this, starting with
        -- position(B'1' in (va # vb)), but go with the simple
        -- implementation for now.
        DECLARE
            i integer   := 1;
            va varbit   := ($1).ix;
            vb varbit   := ($2).ix;
            la integer  := length(va);
            lb integer  := length(vb);
            ab bit;
            bb bit;
        BEGIN
            WHILE i <= la AND i <= lb LOOP
                ab := substring(va from i for 1);
                bb := substring(vb from i for 1);

                -- inc before testing
                i := i + 1;
                
                CASE
                    WHEN ab = bb    THEN CONTINUE;
                    WHEN ab = B'0'  THEN RETURN -1;
                    ELSE            RETURN 1;
                END CASE;
            END LOOP;

            CASE
                WHEN la = lb THEN 
                    RETURN 0;
                WHEN substring(vb from i for 1) = B'1' THEN
                    RETURN -1;
                WHEN substring(va from i for 1) = B'0' THEN
                    RETURN -1;
                ELSE
                    RETURN 1;
            END CASE;
        END;
    $$;

CREATE FUNCTION ordered_lt (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) < 0 $$;

CREATE FUNCTION ordered_le (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) <= 0 $$;

CREATE FUNCTION ordered_eq (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) = 0 $$;

CREATE FUNCTION ordered_ne (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) <> 0 $$;

CREATE FUNCTION ordered_ge (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) >= 0 $$;

CREATE FUNCTION ordered_gt (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) > 0 $$;

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

GRANT USAGE ON SCHEMA ordered1 TO PUBLIC;
GRANT EXECUTE 
    ON FUNCTION 
        before(ordered),
        after(ordered),
        ordered_cmp(ordered, ordered),
        ordered_lt(ordered, ordered),
        ordered_le(ordered, ordered),
        ordered_ge(ordered, ordered),
        ordered_gt(ordered, ordered),
        ordered_eq(ordered, ordered),
        ordered_ne(ordered, ordered)
    TO PUBLIC;

COMMIT;
