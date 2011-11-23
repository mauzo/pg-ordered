--
-- value.sql - functions for manipulating ordered values
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

CREATE FUNCTION _new_ordered_val (ordered)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$
        SELECT ROW(
            $1.rel, nextval(
                pg_get_serial_sequence(
                    $1.rel::regclass::text, 'id'
                )::regclass
            )
        )::ordered;
    $$;

CREATE FUNCTION _insert (o ordered, before boolean)
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
            rv      := _new_ordered_val(o);
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
    AS $$ SELECT _insert($1, TRUE) $$;

CREATE FUNCTION after (ordered)
    RETURNS ordered
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE sql
    AS $$ SELECT _insert($1, FALSE) $$;
