--
-- value.sql - functions for manipulating ordered values
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

CREATE FUNCTION _new_ordered_val (regclass)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$
        SELECT ROW(
            $1, nextval(pg_get_serial_sequence($1::text, 'id'))
        )::ordered;
    $$;

CREATE FUNCTION _before_after (o ordered, before boolean)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            rv      ordered;
            prev    integer;
            subst   text[];
        BEGIN
            rv      := _new_ordered_val(o.rel);
            subst   := ARRAY[ '$ord$', o.rel::regclass::text ];

            EXECUTE do_substs(subst, $$
                SELECT id FROM $ord$
                    WHERE parent = $1 AND first = $2
            $$) INTO prev USING o.id, before;

            -- Update the existing value first, since the FK is
            -- deferrable but the unique isn't.
            IF prev IS NOT NULL THEN
                RAISE NOTICE 'moving % -> % to %',
                    prev, o.id, rv.id;
                EXECUTE do_substs(subst, $$
                    UPDATE $ord$ SET parent = $1
                        WHERE id = $2
                $$) USING rv.id, prev;
            END IF;

            EXECUTE do_substs(subst, $$
                INSERT INTO $ord$ (id, parent, first)
                    VALUES ($1, $2, $3)
            $$) USING rv.id, o.id, before;

            RETURN rv;
        END;
    $fn$;

CREATE FUNCTION before (ordered)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT _before_after($1, TRUE) $$;

CREATE FUNCTION after (ordered)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT _before_after($1, FALSE) $$;

CREATE FUNCTION _first_last (ord regclass, first boolean)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            subst   text[];
            rv      ordered;
            old     integer;
        BEGIN
            subst   := ARRAY[ '$ord$', ord::text ];
            rv      := _new_ordered_val(ord);

            EXECUTE do_substs(subst, $old$
                WITH RECURSIVE
                    parent (id, depth) AS (
                        SELECT id, 0 FROM $ord$ WHERE parent IS NULL
                        UNION ALL
                        SELECT c.id, p.depth + 1
                            FROM $ord$ c JOIN parent p
                                ON c.parent = p.id
                            WHERE c.first = $1
                    )
                SELECT id FROM parent
                    ORDER BY depth DESC
                    LIMIT 1
            $old$) INTO old USING first;

            EXECUTE do_substs(subst, $ins$
                INSERT INTO $ord$ (id, parent, first)
                    VALUES (
                        $1, $2, 
                        CASE WHEN $2 IS NULL THEN NULL ELSE $3 END
                    )
            $ins$) USING rv.id, old, first;

            RETURN rv;
        END;
    $fn$;

CREATE FUNCTION ordered_first (regclass)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT _first_last($1, TRUE) $$;

CREATE FUNCTION ordered_last (regclass)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT _first_last ($1, FALSE) $$;

-- These two functions are something like the two halves of a cascading
-- FK, except (1) the actual FK is hidden inside a composite type and
-- (2) the cascading goes the wrong way, since inserts go into the
-- ordering first but deletes go into the main table.

CREATE FUNCTION ordered_insert ()
    RETURNS trigger
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            att     name        := TG_ARGV[0];
            ord     regclass    := TG_ARGV[1];
            rel     regclass;
            id      integer;
            inord   boolean;
        BEGIN
            EXECUTE do_substs(
                ARRAY[ '$att$', quote_ident(att) ],
                -- it appears to be impossible to SELECT a composite
                -- value INTO a composite variable
                $$ SELECT ($1.$att$).rel, ($1.$att$).id $$
            ) INTO rel, id USING NEW;
            RAISE NOTICE 'rel: %, id: %', rel, id;
            
            IF rel <> ord THEN
                RAISE EXCEPTION 'ordered value is from "%", not "%"',
                    rel, ord;
            END IF;

            EXECUTE do_substs(
                ARRAY[ '$ord$', ord::text ],
                $$ SELECT TRUE FROM $ord$ WHERE id = $1 $$
            ) INTO inord USING id;

            IF inord IS NULL THEN
                RAISE EXCEPTION 'id % is not in ordering "%"',
                    id, ord;
            END IF;

            RETURN NEW;
        END;
    $fn$;

CREATE FUNCTION _ordering_gc 
    (ord regclass, id integer, rel regclass, att name)
    RETURNS void
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            live    boolean;            
            subst   text[];
        BEGIN
            subst := ARRAY[
                '$ord$',    ord::text,
                '$rel$',    rel::text,
                '$att$',    att::text
            ];

            EXECUTE do_substs(subst, $n$
                SELECT TRUE FROM $rel$
                    WHERE ($att$).id = $1
            $n$) INTO live USING id;

            IF live IS NULL THEN
                RAISE NOTICE 'deleting % from %', id, ord;
                EXECUTE do_substs(subst, $del$
                    DELETE FROM $ord$ WHERE id = $1
                $del$) USING id;
            END IF;
        END;
    $fn$;

CREATE FUNCTION ordered_delete ()
    RETURNS trigger
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            att     name    := TG_ARGV[0];
            val     ordered;
        BEGIN
            EXECUTE do_substs(
                ARRAY[ '$att$', quote_ident(att) ],
                $$ SELECT $1.$att$ $$
            ) INTO val USING OLD;

            PERFORM _ordering_gc(val.rel, val.id, TG_RELID, att);
            RETURN NULL;
        END;
    $fn$;

CREATE FUNCTION ordering_insert ()
    RETURNS trigger
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            rel     regclass    := TG_ARGV[0];
            att     name        := TG_ARGV[1];
            val     integer;
        BEGIN
            EXECUTE 'SELECT $1.id' INTO val USING NEW;
            PERFORM _ordering_gc(TG_RELID, val, rel, att);
            RETURN NULL;
        END;
    $fn$;
