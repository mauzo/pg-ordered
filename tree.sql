--
-- tree.sql - functions for creating the ordered trees
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

CREATE FUNCTION create_ordering (rel name, nsp name DEFAULT current_schema)
    RETURNS oid
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            qnsp text := quote_ident(nsp);
            qrel text := quote_ident(rel);
            qual text := qnsp || '.' || qrel;
        BEGIN
            PERFORM do_execs(
                ARRAY[ 
                    '$rel$',    qrel,
                    '$nsp$',    qnsp,
                    '$qual$',   qual
                ],
                ARRAY[
                    $cr$
                        CREATE TABLE $qual$ (
                            id      serial,
                            parent  integer,
                            first   boolean,
                            
                            PRIMARY KEY (id),
                            FOREIGN KEY (parent) 
                                REFERENCES $qual$ (id)
                                -- so we can manipulate the tree
                                DEFERRABLE INITIALLY DEFERRED,
                            UNIQUE (parent, first),
                            CHECK (first IS NOT NULL OR parent IS NULL)
                        )
                    $cr$,
                    -- ensure the tree only has one root
                    $ix$
                        CREATE UNIQUE INDEX $rel$_root_key
                            ON $qual$ ((1))
                            WHERE parent IS NULL
                    $ix$
                ]
            );

            RETURN qual::regclass::oid;
        END;
    $fn$;

CREATE FUNCTION _verify_att (a pg_attribute)
    RETURNS int2
    STABLE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            rel text := (a).attrelid::regclass;
        BEGIN
            IF (a).attnum <= 0 THEN
                RAISE EXCEPTION 'column number must be positive';
            END IF;
            IF (a).attisdropped THEN
                RAISE EXCEPTION 'column % of "%" has been dropped',
                    (a).attnum, rel;
            END IF;
            IF (a).atttypid <> 'ordered'::regtype THEN
                RAISE EXCEPTION
                    'column "%" of "%" is not of type "ordered"',
                    (a).attname, rel;
            END IF;

            RETURN (a).attnum;
        END;
    $fn$;

CREATE FUNCTION _set_ordering_for (ord oid, rel oid, att int2)
    RETURNS void
    VOLATILE
    SET search_path TO ordered1, pg_temp
    SECURITY DEFINER
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            ordown  oid;
            relown  oid;
            cls     oid     := 'pg_class'::regclass::oid;
        BEGIN
            SELECT relowner INTO ordown FROM pg_class WHERE oid = ord;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'relation % does not exist', ord;
            END IF;

            IF NOT pg_has_role(session_user, ordown, 'MEMBER') THEN
                RAISE EXCEPTION 'must be owner of relation "%"',
                    ord::regclass;
            END IF;

            IF rel IS NOT NULL THEN
                SELECT relowner INTO relown FROM pg_class WHERE oid = rel;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'relation % does not exist', rel;
                END IF;

                IF relown <> ordown THEN
                    RAISE EXCEPTION 
                        'ordering must have same owner as table '
                        'it is linked to';
                END IF;

                PERFORM _verify_att(a) FROM pg_attribute a
                    WHERE attrelid = rel AND attnum = att;

                IF NOT FOUND THEN
                    RAISE EXCEPTION 'column % of "%" does not exist',
                        att, rel::regclass;
                END IF;
            END IF;

            DELETE FROM pg_depend
                WHERE classid = cls AND objid = ord
                    -- the schema at least has a 'n' dep on us
                    -- don't delete it by mistake
                    AND refclassid = cls AND deptype = 'a';
    
            IF rel IS NOT NULL THEN
                INSERT INTO pg_depend (
                    classid, objid, objsubid,
                    refclassid, refobjid, refobjsubid,
                    deptype
                ) VALUES (
                    cls, ord, 0,
                    cls, rel, att,
                    'a'
                );
            END IF;
        END;
    $fn$;

CREATE FUNCTION set_ordering_for (ord regclass, rel regclass, att name)
    RETURNS void
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            att_num int2;
        BEGIN
            SELECT _verify_att(a)
                INTO att_num
                FROM pg_attribute a
                WHERE a.attrelid = rel AND a.attname = att;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'column "%" of "%" does not exist',
                    rel, att;
            END IF;

            PERFORM _set_ordering_for(ord, rel, att_num);
            PERFORM do_execs(
                ARRAY[
                    '$ord$', quote_literal(ord),
                    '$rel$', rel::text,
                    '$att$', att::text
                ],
                ARRAY[
                    $chk$ 
                        ALTER TABLE $rel$ 
                            ADD CHECK (($att$).rel = $ord$::regclass)
                    $chk$
                ]
            );
        END;
    $fn$;


CREATE FUNCTION create_ordering_for (rel regclass, att name)
    RETURNS oid
    VOLATILE
    SET search_path FROM CURRENT
    LANGUAGE plpgsql
    AS $fn$
        DECLARE
            nsp     name;
            rel_nm  name;
            ord_nm  name;
            ord_oid oid;
        BEGIN
            SELECT r.relname, n.nspname
                INTO rel_nm, nsp
                FROM pg_class r
                    JOIN pg_namespace n ON r.relnamespace = n.oid
                WHERE r.oid = rel;
            
            IF NOT FOUND THEN
                RAISE EXCEPTION 'relation "%" does not exist', rel;
            END IF;

            ord_nm := rel_nm || '_' || att || '_ord';

            SELECT create_ordering(ord_nm, nsp) INTO ord_oid;
            PERFORM set_ordering_for(ord_oid, rel, att);

            RETURN ord_oid;
        END;
    $fn$;
