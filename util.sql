--
-- util.sql - utility functions
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

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

