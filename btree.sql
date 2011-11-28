--
-- btree.sql - a btree-ish data structure in SQL
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

CREATE TABLE btree (
    id      serial      PRIMARY KEY,
    kids    integer[]   DEFAULT '{}' NOT NULL,
    keys    integer[]   DEFAULT '{}' NOT NULL
);
CREATE INDEX btree_keys ON btree USING gin (keys);
CREATE INDEX btree_kids ON btree USING gin (kids);

-- since this is just a temporary arrangement until users can create
-- their own trees, allow full access.
GRANT ALL ON btree TO PUBLIC;
GRANT ALL ON SEQUENCE btree_id_seq TO PUBLIC;

CREATE TYPE btree_x AS (
    id integer,
    nk integer,
    ks integer[],
    nd integer,
    ds integer[],
    ix integer
);

-- btree_x functions

CREATE FUNCTION expand(btree) RETURNS btree_x
    LANGUAGE sql IMMUTABLE
    AS $fn$
        SELECT $1.id, coalesce(array_length($1.keys, 1), 0), $1.keys,
            array_length($1.kids, 1), $1.kids, NULL::integer;
    $fn$;

CREATE FUNCTION reset(btree_x) RETURNS btree_x
    LANGUAGE sql IMMUTABLE
    AS $fn$
        SELECT $1.id, coalesce(array_length($1.ks, 1), 0), $1.ks,
            array_length($1.ds, 1), $1.ds, $1.ix;
    $fn$;

CREATE FUNCTION update(n btree_x) RETURNS void
    LANGUAGE plpgsql VOLATILE
    AS $fn$
        BEGIN
            IF n.ks = '{}' THEN
                DELETE FROM btree WHERE id = n.id;
            ELSE
                UPDATE btree SET keys = n.ks, kids = n.ds
                    WHERE id = n.id;
            END IF;
        END;
    $fn$;

-- array functions

CREATE FUNCTION splice(integer[], integer[], integer) RETURNS integer[]
    LANGUAGE sql IMMUTABLE
    AS $fn$ 
        SELECT $1[1:$3-1] || $2 || $1[$3+1:array_length($1, 1)];
    $fn$;

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
            p := expand(b) FROM btree b WHERE ARRAY[n.id] <@ kids;

            IF p IS NULL THEN
                -- The root is allowed to underflow. If it empties
                -- completely it will be deleted when the caller calls
                -- update().
                RETURN;
            END IF;

            p.ix := ix(p.ds, n.id);

            << switch >>
            BEGIN
                IF p.ix > 1 THEN
                    s := expand(b) FROM btree b
                        WHERE id = p.ds[p.ix - 1];

                    IF s.nk > 4 THEN
                        n.ks          := p.ks[p.ix-1] || n.ks;
                        p.ks[p.ix-1]  := s.ks[s.nk];
                        s.ks          := s.ks[1:s.nk-1];

                        IF n.ds <> '{}' THEN
                            n.ds      := s.ds[s.nd] || n.ds;
                            s.ds      := s.ds[1:s.nd-1];
                        END IF;

                        EXIT switch;
                    END IF;
                END IF;

                IF p.ix < p.nd THEN
                    s := expand(b) FROM btree b
                        WHERE id = p.ds[p.ix + 1];

                    IF s.nk > 4 THEN
                        n.ks        := n.ks || p.ks[p.ix];
                        p.ks[p.ix]  := s.ks[1];
                        s.ks        := s.ks[2:s.nk];

                        IF n.ds <> '{}' THEN
                            n.ds    := n.ds || s.ds[1];
                            s.ds    := s.ds[2:s.nd];
                        END IF;

                        EXIT switch;
                    END IF;
                END IF;
            
                -- one of the two cases above matched (non-root
                -- parent has at least 5 kids, root has at least
                -- two) so we don't need to re-select.
                IF s IS NULL THEN
                    RAISE 'panic: merge % with no siblings', n;
                END IF;

                IF p.ix < p.nd THEN
                    n.ks  := n.ks || p.ks[p.ix] || s.ks;
                    n.ds  := n.ds || s.ds;
                    p.ks  := p.ks[1:p.ix-1] || p.ks[p.ix+1:p.nk];
                    p.ds  := p.ds[1:p.ix] || p.ds[p.ix+2:p.nd];
                ELSE
                    n.ks  := s.ks || p.ks[p.ix-1] || n.ks;
                    n.ds  := s.ds || n.ds;
                    p.ks  := p.ks[1:p.ix-2];
                    p.ds  := p.ds[1:p.ix-2] || p.ds[p.ix];
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
            c       btree_x;
            i       integer;
            l       integer;
        BEGIN
            n := expand(b) FROM btree b WHERE ARRAY[k] <@ keys;
            IF n IS NULL THEN
                RAISE 'not in btree: %', k;
            END IF;

            n.ix := ix(n.ks, k);

            RAISE NOTICE 'deleting % at %/% in node %', k, n.ix, n.nk, n.id;

            IF n.ds = '{}' THEN
                n.ks := n.ks[1:n.ix-1] || n.ks[n.ix+1:n.nk];
                n := reset(n);

                IF n.nk < 4 THEN
                    n := underflow(n);
                END IF;
            ELSE
                c := expand(last_child(n.ds[n.ix]));
                RAISE NOTICE 'last child: % (%)', c.id, c.ks[c.nk];

                n.ks[n.ix]  := c.ks[c.nk];
                c.ks        := c.ks[1:c.nk - 1];
                c := reset(c);

                IF c.nk < 4 THEN
                    c := underflow(c);
                END IF;

                PERFORM update(c);
            END IF;

            PERFORM update(n);
        END;
    $$;

CREATE FUNCTION insert(
        n integer, before integer, iskey boolean DEFAULT TRUE, 
        val integer DEFAULT NULL, d integer DEFAULT NULL
    ) RETURNS integer
    LANGUAGE plpgsql VOLATILE
    AS $$
        DECLARE
            seq     regclass;
            k       integer;
            i       integer;
            ks      integer[];
            ds      integer[];
            p       integer;
            len     integer;
            new     integer;
        BEGIN
            seq := pg_get_serial_sequence('btree', 'id');
            k   := coalesce(val, nextval(seq));

            SELECT keys, kids INTO ks, ds
                FROM btree WHERE id = n;
            IF NOT FOUND THEN 
                RAISE '% not a btree node', n; 
            END IF;

            len := coalesce(array_length(ks, 1), 0);
            i   := CASE 
                WHEN before IS NULL THEN len + 1
                WHEN iskey          THEN ix(ks, before)
                ELSE ix(ds, before)
            END;
            IF i = 0 THEN 
                RAISE '% not found in node %', before, n;
            END IF;

            ks := ks[1:i - 1] || k || ks[i:len];
            IF d IS NOT NULL THEN
                -- we always split leftwards
                ds := ds[1:i - 1] || d || ds[i:len + 1];
            END IF;

            IF len < 8 THEN
                UPDATE btree SET keys = ks, kids = ds WHERE id = n;
            ELSE
                new := nextval(seq);
                SELECT id INTO p FROM btree WHERE ARRAY[n] <@ kids;

                IF p IS NULL THEN
                    INSERT INTO btree (keys, kids)
                        VALUES (ARRAY[ ks[5] ], ARRAY[ new, n ])
                        RETURNING id INTO p;
                ELSE
                    PERFORM insert(p, n, FALSE, ks[5], new);
                END IF;

                UPDATE btree 
                    SET keys = ks[6:9], kids = ds[6:10]
                    WHERE id = n;
                INSERT INTO btree (id, keys, kids) 
                    VALUES (new, ks[1:4], ds[1:5]);
            END IF;

            RETURN k;
        END;
    $$;

-- search

CREATE FUNCTION first_child(btree_x) RETURNS btree_x
    LANGUAGE sql
    AS $fn$
        WITH RECURSIVE
            parent (bt, kids, depth) AS (
                SELECT b, b.kids, 0::integer
                    FROM btree b
                    WHERE b.id = $1.id
                UNION ALL
                SELECT c, c.kids, p.depth + 1
                    FROM parent p
                        JOIN btree c
                            ON c.id = p.kids[1]
            )
            SELECT expand(bt) FROM parent 
                ORDER BY depth DESC LIMIT 1
    $fn$;

CREATE FUNCTION last_child(btree_x) RETURNS btree_x
    LANGUAGE sql
    AS $fn$
        WITH RECURSIVE
            parent (bt, kids, depth) AS (
                SELECT b, b.kids, 0::integer
                    FROM btree b
                    WHERE b.id = $1.id
                UNION ALL
                SELECT c, c.kids, p.depth + 1
                    FROM parent p
                        JOIN btree c
                            ON c.id = p.kids[array_length(p.kids, 1)]
            )
            SELECT expand(bt) FROM parent 
                ORDER BY depth DESC LIMIT 1
    $fn$;

CREATE FUNCTION last_child(integer) RETURNS btree
    LANGUAGE sql
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

CREATE FUNCTION last_children(btree_x) RETURNS SETOF btree_x
    LANGUAGE sql
    AS $fn$
        WITH RECURSIVE
            parent (bt, kids, depth) AS (
                SELECT b, b.kids, 0::integer
                    FROM btree b
                    WHERE b.id = $1.id
                UNION ALL
                SELECT c, c.kids, p.depth + 1
                    FROM parent p
                        JOIN btree c
                            ON c.id = p.kids[array_length(p.kids, 1)]
            )
            SELECT expand(bt) FROM parent
    $fn$;
