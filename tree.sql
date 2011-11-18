--
-- tree.sql - functions for creating the ordered trees
-- Part of an implementation of an 'ordered' data type.
--
-- This file should not be run directly. Run ordered.sql instead.
--
-- Copyright 2011 Ben Morrow <ben@morrow.me.uk>
-- Released under the 2-clause BSD licence.
--

CREATE FUNCTION create_ordering (nm name)
    RETURNS void
    VOLATILE
    LANGUAGE plpgsql
    AS $fn$
        BEGIN
            -- We can't SET search_path since that will create objects in
            -- the wrong schema. That means explicitly qualifying
            -- everything (fortunately there isn't much to qualify).
            PERFORM ordered1.do_execs(
                ARRAY[ '$tab$', nm ],
                ARRAY[
                    $cr$
                        CREATE TABLE $tab$ (
                            id      serial,
                            parent  integer,
                            first   boolean,
                            
                            PRIMARY KEY (id),
                            FOREIGN KEY (parent)
                                REFERENCES $tab$ (id)
                                -- so we can manipulate the tree
                                DEFERRABLE INITIALLY DEFERRED,
                            UNIQUE (parent, first),
                            CHECK (first IS NOT NULL OR parent IS NULL)
                        )
                    $cr$,
                    -- ensure the tree only has one root
                    $ix$
                        CREATE UNIQUE INDEX $tab$_root_key
                            ON $tab$ ((1))
                            WHERE parent IS NULL
                    $ix$
                ]
            );
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
                RAISE EXCEPTION 'must be owner of relation %',
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

