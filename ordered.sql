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

-- Utility functions

CREATE FUNCTION do_execs (subst text[], cmds text[])
    RETURNS void
    VOLATILE
    LANGUAGE plpgsql
    AS $$
        DECLARE
            cmd     text;
            i       integer;
        BEGIN
            -- XXX is it worth qualifying these functions?
            FOR cmd IN SELECT unnest(cmds) LOOP
                FOR i IN 1 .. array_length(subst, 1) BY 2 LOOP
                    cmd := replace(cmd, subst[i], subst[i + 1]);
                END LOOP;
                --RAISE NOTICE 'exec: [%]', cmd;
                EXECUTE cmd;
            END LOOP;
        END;
    $$;

-- Trees

CREATE FUNCTION create_ordering (nm name)
    RETURNS void
    VOLATILE
    LANGUAGE plpgsql
    AS $fn$
        BEGIN
            -- We can't SET search_path since that will create objects in
            -- the wrong schema. That means explicitly qualifying
            -- everything (fortunately there isn't much to qualify).
            PERFORM ordered1.do_execs(
                ARRAY[ '$tab$', nm ],
                ARRAY[
                    $cr$
                        CREATE TABLE $tab$ (
                            id      serial,
                            parent  integer,
                            first   boolean,
                            
                            PRIMARY KEY (id),
                            FOREIGN KEY (parent)
                                REFERENCES $tab$ (id)
                                -- so we can manipulate the tree
                                DEFERRABLE INITIALLY DEFERRED,
                            UNIQUE (parent, first),
                            CHECK (first IS NOT NULL OR parent IS NULL)
                        )
                    $cr$,
                    -- ensure the tree only has one root
                    $ix$
                        CREATE UNIQUE INDEX $tab$_root_key
                            ON $tab$ ((1))
                            WHERE parent IS NULL
                    $ix$
                ]
            );
        END;
    $fn$;

CREATE FUNCTION _set_ordering_for (ord oid, rel oid, att int2)
    RETURNS void
    VOLATILE
    SET search_path TO ordered1, pg_temp
    SECURITY DEFINER
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            cls oid := 'pg_class'::regclass::oid;
        BEGIN
            DELETE FROM pg_depend
                WHERE classid = cls AND objid = ord
                    -- the schema at least has a 'n' dep on us
                    -- don't delete it by mistake
                    AND refclassid = cls AND deptype = 'a';

            IF rel IS NOT NULL THEN
                INSERT INTO pg_depend (
                    classid, objid, objsubid,
                    refclassid, refobjid, refobjsubid,
                    deptype
                ) VALUES (
                    cls, ord, 0,
                    cls, rel, att,
                    'a'
                );
            END IF;
        END;
    $fn$;

CREATE FUNCTION ancestors (o ordered)
    RETURNS TABLE ( id integer, depth integer, cmp integer )
    STABLE STRICT
    SET search_path FROM CURRENT
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

-- Index functions

CREATE FUNCTION ordered_cmp (a ordered, b ordered)
    RETURNS integer
    STABLE STRICT
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

CREATE FUNCTION ordered_lt (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) < 0 $$;

CREATE FUNCTION ordered_le (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) <= 0 $$;

CREATE FUNCTION ordered_eq (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) = 0 $$;

CREATE FUNCTION ordered_ne (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) <> 0 $$;

CREATE FUNCTION ordered_ge (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT ordered_cmp($1, $2) >= 0 $$;

CREATE FUNCTION ordered_gt (ordered, ordered)
    RETURNS boolean
    IMMUTABLE STRICT
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


-- Ordered values

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

-- Permissions

GRANT USAGE ON SCHEMA ordered1 TO PUBLIC;
GRANT EXECUTE 
    ON FUNCTION 
        create_ordering(name),
        _set_ordering_for(oid, oid, int2),

        --before(ordered),
        --after(ordered),

        ancestors(ordered),
        ordered_cmp(ordered, ordered),
        ordered_lt(ordered, ordered),
        ordered_le(ordered, ordered),
        ordered_ge(ordered, ordered),
        ordered_gt(ordered, ordered),
        ordered_eq(ordered, ordered),
        ordered_ne(ordered, ordered)
    TO PUBLIC;

COMMIT;
