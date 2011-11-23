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
