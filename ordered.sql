BEGIN;

-- Schema

DROP SCHEMA IF EXISTS ordered1 CASCADE;
CREATE SCHEMA ordered1;
SET search_path TO ordered1;

-- Data types

CREATE TYPE ordered AS (
    rel     oid,
    ix      varbit
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
            FOR cmd IN SELECT unnest(cmds) LOOP
                FOR i IN 1 .. array_length(subst, 1) BY 2 LOOP
                    cmd := replace(cmd, subst[i], subst[i + 1]);
                END LOOP;
                RAISE NOTICE 'exec: [%]', cmd;
                EXECUTE cmd;
            END LOOP;
        END;
    $$;

-- Trees

CREATE FUNCTION create_tree (nm name)
    RETURNS void
    VOLATILE
    LANGUAGE plpgsql
    AS $fn$
        BEGIN
            PERFORM do_execs(
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

CREATE FUNCTION ancestors (ord regclass, ix integer)
    RETURNS TABLE ( id integer, depth integer, cmp integer )
    STABLE STRICT
    LANGUAGE plpgsql
    AS $fn$
        BEGIN
            RETURN QUERY EXECUTE replace($an$
                WITH RECURSIVE
                    child ( id, parent, first, depth, cmp ) 
                    AS (
                        SELECT
                            id, parent, first, 0, 0
                            FROM $ord$
                            WHERE id = $1
                        UNION ALL
                        SELECT
                            o.id, o.parent, o.first, c.depth + 1,
                            CASE WHEN c.first THEN -1 ELSE 1 END
                        FROM child c
                            JOIN $ord$ o
                            ON o.id = c.parent
                    )
                SELECT id, depth, cmp FROM child
            $an$, '$ord$', ord::text)
            USING ix;

            RETURN;
        END;
    $fn$;

CREATE FUNCTION tree_cmp (ord regclass, a integer, b integer)
    RETURNS integer
    STABLE STRICT
    LANGUAGE sql
    AS $fn$
        SELECT CASE
            -- shortcircuit the simple case
            WHEN $2 = $3 THEN 0
            ELSE (
                SELECT btint4cmp(a.cmp, b.cmp)
                    FROM ancestors($1, $2) a
                        JOIN ancestors($1, $3) b
                        ON a.id = b.id
                    ORDER BY a.depth
                    LIMIT 1
            )
        END
    $fn$;

-- Subsidiary tables

CREATE FUNCTION create_ordering (nm name)
    RETURNS void
    VOLATILE
    -- *no* set search_path, since we are creating a table
    LANGUAGE plpgsql
    AS $fn$ 
        BEGIN
            PERFORM do_execs(
                ARRAY[ '$tab$', nm ],
                ARRAY[ $cr$ 
                    CREATE TABLE $tab$ (
                        rel     oid,
                        att     int2,
                        first   varbit,
                        last    varbit
                    ) 
                $cr$, $ins$ 
                    INSERT INTO $tab$ (first, last)
                        VALUES (B'', B'') 
                $ins$ ]
            );
        END;
    $fn$;

CREATE FUNCTION set_ordering_for (ord oid, rel oid, att int2)
    RETURNS void
    VOLATILE
    SET search_path TO ordered1, pg_temp
    SECURITY DEFINER
    LANGUAGE plpgsql
    AS $fn$
        BEGIN
            PERFORM do_execs(
                ARRAY[
                    '$ord$', ord::text,
                    '$onm$', ord::regclass::text,
                    '$rel$', rel::text,
                    '$att$', att::text,
                    '$cls$', 'pg_class'::regclass::oid::text
                ],
                ARRAY[
                    $upd$ UPDATE $onm$ SET rel = $rel$, att = $att$ $upd$,
                    $dep$
                        INSERT INTO pg_depend (
                            classid, objid, objsubid,
                            refclassid, refobjid, refobjsubid,
                            deptype
                        ) VALUES (
                            $cls$, $ord$, 0,
                            $cls$, $rel$, $att$,
                            'i'
                        )
                    $dep$
                ]
            );
        END;
    $fn$;

-- Ordered values

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

-- Index functions

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

-- Permissions

GRANT USAGE ON SCHEMA ordered1 TO PUBLIC;
GRANT EXECUTE 
    ON FUNCTION 
        create_tree(name),
        ancestors(regclass, integer),
        tree_cmp(regclass, integer, integer),

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
