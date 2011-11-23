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

