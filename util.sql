--
-- util.sql - utility functions
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

CREATE FUNCTION do_substs (subst text[], cmd text)
    RETURNS text
    IMMUTABLE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $$
        DECLARE
            rv  text    := cmd;
        BEGIN
            FOR i IN 1 .. array_length(subst, 1) BY 2 LOOP
                rv := replace(rv, subst[i], subst[i + 1]);
            END LOOP;

            RETURN rv;
        END;
    $$;

CREATE FUNCTION do_execs (subst text[], cmds text[])
    RETURNS void
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $$
        DECLARE
            cmd     text;
            i       integer;
        BEGIN
            FOR cmd IN SELECT unnest(cmds) LOOP
                cmd := do_substs(subst, cmd);
                --RAISE NOTICE 'exec: [%]', cmd;
                EXECUTE cmd;
            END LOOP;
        END;
    $$;

CREATE FUNCTION _do (sql text) RETURNS void
    LANGUAGE plpgsql VOLATILE
    AS $fn$
        DECLARE
            pgt     oid;
        BEGIN
            -- We create the function in pg_temp, since it's not certain
            -- we've got write access anywhere else. This means the call
            -- needs to be fully-qualified, since functions are never
            -- looked up in pg_temp. If pg_temp doesn't exist yet, we
            -- need to autoviv it by creating and dropping a temp table.

            pgt := pg_my_temp_schema();

            IF pgt = 0 THEN
                CREATE TEMP TABLE "_do autoviv pg_temp" ();
            END IF;

            PERFORM 1 FROM pg_proc
                WHERE proname = '_do anon block'
                    AND pronamespace = pg_my_temp_schema();
            IF FOUND THEN
                RAISE 'recursive _do not supported';
            END IF;

            EXECUTE ordered1.do_substs(
                array[ '$sql$', pg_catalog.quote_literal(sql) ],
                $do$
                    CREATE FUNCTION pg_temp."_do anon block" () RETURNS void
                        LANGUAGE plpgsql VOLATILE
                        AS $sql$;
                $do$
            );
            PERFORM pg_temp."_do anon block"();
            DROP FUNCTION pg_temp."_do anon block" ();

            -- we'll keep the temp table around until after we've
            -- finished with pg_temp, just in case Pg ever decides to
            -- start dropping it when it isn't needed any more.
            IF pgt = 0 THEN
                DROP TABLE "_do autoviv pg_temp";
            END IF;
        END;
    $fn$;
