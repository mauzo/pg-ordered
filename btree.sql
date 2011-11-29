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
    kids    integer[]   DEFAULT '{}' NOT NULL,
    leaf    boolean     DEFAULT TRUE NOT NULL
);
CREATE INDEX btree_kids ON btree USING gin (kids);

-- since this is just a temporary arrangement until users can create
-- their own trees, allow full access.
GRANT ALL ON btree TO PUBLIC;
GRANT ALL ON SEQUENCE btree_id_seq TO PUBLIC;

CREATE TYPE btree_x AS (
    id      integer,
    nk      integer,
    ks      integer[],
    leaf    boolean,
    ix      integer
);

-- btree_x functions

CREATE FUNCTION btree_x(btree) RETURNS btree_x
    LANGUAGE sql IMMUTABLE
    AS $fn$
        SELECT $1.id, coalesce(array_length($1.kids, 1), 0), $1.kids,
            $1.leaf, NULL::integer; 
    $fn$;
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

CREATE FUNCTION reset(btree_x) RETURNS btree_x
    LANGUAGE sql IMMUTABLE
    AS $fn$
        SELECT $1.id, coalesce(array_length($1.ks, 1), 0), $1.ks,
            $1.leaf, $1.ix;
    $fn$;

CREATE FUNCTION update(INOUT n btree_x)
    LANGUAGE plpgsql VOLATILE
    AS $fn$
        BEGIN
            CASE
                WHEN n.id IS NULL THEN
                    INSERT INTO btree (kids, leaf)
                        VALUES (n.ks, n.leaf)
                        RETURNING id INTO n.id;

                WHEN n.ks = '{}' THEN
                    DELETE FROM btree WHERE id = n.id;

                ELSE
                    UPDATE btree SET kids = n.ks
                        WHERE id = n.id;
            END CASE;
        END;
    $fn$;

-- array functions

CREATE FUNCTION ix(anyarray, anyelement) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $fn$
        SELECT n 
            FROM generate_series(1, array_length($1, 1)) s (n)
            WHERE $1[n] = $2
    $fn$;

-- insert and delete

CREATE FUNCTION underflow(INOUT n btree_x)
    LANGUAGE plpgsql VOLATILE
    AS $$
        DECLARE
            p   btree_x;
            s   btree_x;
        BEGIN
            p := b::btree_x FROM btree b 
                WHERE ARRAY[n.id] <@ kids AND NOT leaf;

            IF p IS NULL THEN
                -- The root is allowed to underflow. If it empties
                -- completely it will be deleted when the caller calls
                -- update().
                RETURN;
            END IF;

            p.ix := ix(p.ks, n.id);

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
            n := b::btree_x FROM btree b 
                WHERE ARRAY[k] <@ kids AND leaf;
            IF n IS NULL THEN
                RAISE 'not in btree: %', k;
            END IF;

            n.ix := ix(n.ks, k);

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
            seq     regclass;
            k       integer;
            n       btree_x;
            s       btree_x;
            p       btree_x;
        BEGIN
            seq := pg_get_serial_sequence('btree', 'id');
            k   := coalesce(val, nextval(seq));

            IF before IS NULL THEN
                n := last_child(id)::btree_x FROM (
                    SELECT id FROM btree c WHERE NOT EXISTS (
                        SELECT 1 FROM btree px WHERE ARRAY[c.id] <@ px.kids
                    )
                ) r;
                n.ix := n.nk + 1;
            ELSE
                n := b::btree_x FROM btree b
                    WHERE ARRAY[before] <@ kids AND leaf = isk;
                
                IF n IS NULL THEN
                    RAISE 'not in btree: % (%)', before, isk;
                END IF;

                n.ix := ix(n.ks, before);
            END IF;

            RAISE NOTICE 'inserting % into % at %', k, n.id, n.ix;

            n.ks := n.ks[1:n.ix-1] || k || n.ks[n.ix:n.nk];
            n    := reset(n);

            IF n.nk < 8 THEN
                PERFORM update(n);
            ELSE
                s.ks    := n.ks[1:4];
                s.leaf  := n.leaf;
                n.ks    := n.ks[5:8];

                s := update(s);
                n := update(n);

                p := b::btree_x FROM btree b
                    WHERE ARRAY[n.id] <@ kids AND NOT leaf;

                IF p IS NULL THEN
                    RAISE NOTICE 'split %/% to new root', n.id, s.id;
                    p.ks    := ARRAY[ s.id, n.id ];
                    p.leaf  := FALSE;
                    p := update(p);
                    RAISE NOTICE 'new root: %', p.id;
                ELSE
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
