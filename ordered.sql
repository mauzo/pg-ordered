BEGIN;

DROP SCHEMA IF EXISTS ordered1 CASCADE;
CREATE SCHEMA ordered1;
SET search_path TO ordered1;

CREATE TYPE ordered AS (
    rel     oid,
    ix      varbit
);

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
                    WHEN bb = B'0'  THEN RETURN -1;
                    ELSE            RETURN 1;
                END CASE;
            END LOOP;

            CASE
                WHEN la = lb THEN 
                    RETURN 0;
                WHEN substring(vb from i for 1) = B'0' THEN
                    RETURN -1;
                WHEN substring(va from i for 1) = B'1' THEN
                    RETURN -1;
                ELSE
                    RETURN 1;
            END CASE;
        END;
    $$;

COMMIT;
