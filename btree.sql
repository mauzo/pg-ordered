--
-- btree.sql - a btree-ish data structure in SQL
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

-- This is not quite a btree. Since it's never used for search (all
-- lookups by key go through the gin index, which is sorted in a
-- different order) there's no need for any keys in the non-leaf nodes.
-- So, every node has a single array of 'kids': for leaf nodes, these
-- are keys, for non-leaf nodes, these are pointers to child nodes.

CREATE TABLE btree (
    id      serial      PRIMARY KEY,
    kids    integer[]   NOT NULL,
    leaf    boolean,

    CHECK( id = 0 OR leaf IS NOT NULL )
);
CREATE INDEX btree_kids ON btree USING gin (kids);

CREATE FUNCTION reset_btree (integer) RETURNS void
    LANGUAGE sql VOLATILE
    SECURITY DEFINER
    SET search_path FROM CURRENT
    AS $fn$
        TRUNCATE btree RESTART IDENTITY;
        INSERT INTO btree (kids, leaf) VALUES ('{}', true);
        INSERT INTO btree VALUES (0, array[1, $1], NULL);
    $fn$;

-- since this is just a temporary arrangement until users can create
-- their own trees, allow full access.
GRANT ALL ON btree TO PUBLIC;
GRANT ALL ON SEQUENCE btree_id_seq TO PUBLIC;

-- array functions

CREATE FUNCTION idx(anyarray, anyelement) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $fn$
        SELECT n 
            FROM generate_series(1, array_length($1, 1)) s (n)
            WHERE $1[n] = $2
    $fn$;

-- use the optimised version from contrib/intarray if we've got it
SELECT _do($do$
    BEGIN
        CREATE FUNCTION idx (integer[], integer) RETURNS integer
            LANGUAGE C STRICT IMMUTABLE
            AS '$libdir/_int';
    EXCEPTION
        WHEN undefined_file THEN
            NULL;
    END;
$do$);

-- metapage functions

CREATE TYPE btree_meta AS (
    root    integer,
    fill    integer
);

CREATE FUNCTION btmeta () RETURNS btree_meta
    LANGUAGE sql STABLE
    AS $fn$
        SELECT kids[1], kids[2]
            FROM btree
            WHERE id = 0;
    $fn$;

CREATE FUNCTION update (btree_meta) RETURNS btree_meta
    LANGUAGE sql VOLATILE
    AS $fn$
        UPDATE btree SET kids[1] = $1.root, kids[2] = $1.fill
            WHERE id = 0;
        SELECT btmeta();
    $fn$;

-- btree_x functions

CREATE TYPE btree_x AS (
    meta    btree_meta,
    id      integer,
    nk      integer,
    ks      integer[],
    leaf    boolean,
    ix      integer
);

CREATE FUNCTION btree_x (btree, integer) RETURNS btree_x
    LANGUAGE sql IMMUTABLE
    AS $fn$
        SELECT btmeta(), $1.id, 
            coalesce(array_length($1.kids, 1), 0), $1.kids,
            $1.leaf, idx($1.kids, $2);
    $fn$;

CREATE FUNCTION btree_x (btree) RETURNS btree_x
    LANGUAGE sql IMMUTABLE
    AS $fn$ SELECT btree_x($1, NULL::integer); $fn$;

CREATE CAST (btree AS btree_x)
    WITH FUNCTION btree_x (btree)
    AS ASSIGNMENT;

CREATE FUNCTION btree(btree_x) RETURNS btree
    LANGUAGE sql IMMUTABLE
    AS $fn$ 
        SELECT $1.id, $1.ks, $1.leaf;
    $fn$;
CREATE CAST (btree_x AS btree)
    WITH FUNCTION btree (btree_x);

CREATE FUNCTION btfind (integer, boolean) RETURNS btree_x
    LANGUAGE sql STABLE
    AS $fn$
        SELECT btree_x(b, $1) FROM btree b
            WHERE array[$1] <@ b.kids AND b.leaf = $2;
    $fn$;

CREATE FUNCTION reset(btree_x) RETURNS btree_x
    LANGUAGE sql IMMUTABLE
    AS $fn$
        SELECT $1.meta, $1.id, 
            coalesce(array_length($1.ks, 1), 0), $1.ks,
            $1.leaf, $1.ix;
    $fn$;

CREATE FUNCTION update(INOUT n btree_x)
    LANGUAGE plpgsql VOLATILE
    AS $fn$
        DECLARE
            m   btree_meta;
        BEGIN
            m := n.meta;
            CASE
                WHEN n.id IS NULL THEN
                    INSERT INTO btree (kids, leaf)
                        VALUES (n.ks, n.leaf)
                        RETURNING id INTO n.id;

                WHEN n.ks = '{}' THEN
                    IF n.id = m.root AND n.leaf THEN
                        RETURN;
                    END IF;
                    DELETE FROM btree WHERE id = n.id;

                ELSE
                    UPDATE btree SET kids = n.ks
                        WHERE id = n.id;
            END CASE;
        END;
    $fn$;

-- insert and delete

CREATE FUNCTION underflow(INOUT n btree_x)
    LANGUAGE plpgsql VOLATILE
    AS $$
        DECLARE
            m   btree_meta;
            p   btree_x;
            s   btree_x;
        BEGIN
            m := n.meta;

            IF n.id = m.root THEN
                -- A leaf root is allowed to underflow right down to
                -- empty, and never gets deleted. A non-leaf root with
                -- only one child is replaced by its child.
                IF NOT n.leaf AND n.nk = 1 THEN
                    m.root  := n.ks[1];
                    n.ks    := '{}';

                    n.meta  := update(m);
                    n       := reset(n);
                END IF;
                RETURN;
            END IF;

            p := btfind(n.id, false);

            IF p.id IS NULL THEN
                RAISE 'non-root % has no parents', n.id;
            END IF;

            << switch >>
            BEGIN
                IF p.ix > 1 THEN
                    s := b::btree_x FROM btree b
                        WHERE id = p.ks[p.ix - 1];

                    IF s.nk > 4 THEN
                        n.ks    := s.ks[s.nk] || n.ks;
                        s.ks    := s.ks[1:s.nk-1];

                        EXIT switch;
                    END IF;
                END IF;

                IF p.ix < p.nk THEN
                    s := b::btree_x FROM btree b
                        WHERE id = p.ks[p.ix + 1];

                    IF s.nk > 4 THEN
                        n.ks    := n.ks || s.ks[1];
                        s.ks    := s.ks[2:s.nk];

                        EXIT switch;
                    END IF;
                END IF;
            
                -- one of the two cases above matched (non-root
                -- parent has at least 5 kids, root has at least
                -- two) so we don't need to re-select.
                IF s IS NULL THEN
                    RAISE 'panic: merge % with no siblings', n;
                END IF;

                IF p.ix < p.nk THEN
                    n.ks  := n.ks || s.ks;
                    p.ks  := p.ks[1:p.ix] || p.ks[p.ix+2:p.nk];
                ELSE
                    n.ks  := s.ks || n.ks;
                    p.ks  := p.ks[1:p.ix-2] || p.ks[p.ix];
                END IF;

                s.ks := '{}';
                p    := reset(p);

                IF p.nk - 1 < 4 THEN
                    RAISE NOTICE 'recursive underflow on %', p.id;
                    p := underflow(p);
                END IF;
            END;

            PERFORM update(p);
            PERFORM update(s);
        END;
    $$;

CREATE FUNCTION delete(k integer) RETURNS void
    LANGUAGE plpgsql VOLATILE
    AS $$
        DECLARE
            n       btree_x;
        BEGIN
            n := btfind(k, true);
            IF n IS NULL THEN
                RAISE 'not in btree: %', k;
            END IF;

            RAISE NOTICE 'deleting % at %/% in node %', k, n.ix, n.nk, n.id;

            n.ks := n.ks[1:n.ix-1] || n.ks[n.ix+1:n.nk];
            n := reset(n);

            IF n.nk < 4 THEN
                n := underflow(n);
            END IF;

            PERFORM update(n);
        END;
    $$;

CREATE FUNCTION insert(
        before integer, 
        val integer DEFAULT NULL, isk boolean DEFAULT TRUE
    ) RETURNS integer
    LANGUAGE plpgsql VOLATILE
    AS $$
        DECLARE
            m       btree_meta;
            seq     regclass;
            k       integer;
            n       btree_x;
            s       btree_x;
            p       btree_x;
        BEGIN
            m   := btmeta();
            seq := pg_get_serial_sequence('btree', 'id');

            IF before IS NULL THEN
                n := last_child(m.root)::btree_x;
                n.ix := n.nk + 1;
            ELSE
                n := btfind(before, isk);
                
                IF n.id IS NULL THEN
                    RAISE 'not in btree: %', before;
                END IF;
            END IF;

            k := coalesce(val, nextval(seq));
            RAISE NOTICE 'inserting % into % at %', k, n.id, n.ix;

            n.ks := n.ks[1:n.ix-1] || k || n.ks[n.ix:n.nk];
            n    := reset(n);

            IF n.nk < m.fill THEN
                PERFORM update(n);
            ELSE
                s.ks    := n.ks[1:m.fill/2];
                s.leaf  := n.leaf;
                n.ks    := n.ks[(m.fill/2 + 1):m.fill];

                s := update(s);
                n := update(n);

                IF n.id = m.root THEN
                    RAISE NOTICE 'split %/% to new root', n.id, s.id;
                    p.ks    := ARRAY[ s.id, n.id ];
                    p.leaf  := FALSE;
                    p := update(p);

                    RAISE NOTICE 'new root: %', p.id;
                    m.root := p.id;
                    n.meta := update(m);
                ELSE
                    p := btfind(n.id, false);
                    RAISE NOTICE 'split %/% in %', n.id, s.id, p.id;
                    PERFORM insert(n.id, s.id, FALSE);
                END IF;

            END IF;

            RETURN k;
        END;
    $$;

-- search

CREATE FUNCTION last_child(integer) RETURNS btree
    LANGUAGE sql STABLE
    AS $fn$
        WITH RECURSIVE
            parent (bt, depth) AS (
                SELECT b, 0::integer
                    FROM btree b
                    WHERE b.id = $1
                UNION ALL
                SELECT c, p.depth + 1
                    FROM parent p
                        JOIN btree c
                            ON c.id = (p.bt).kids[array_length((p.bt).kids, 1)]
            )
            SELECT bt FROM parent 
                ORDER BY depth DESC LIMIT 1
    $fn$;
