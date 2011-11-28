BEGIN;

SET search_path TO ordered1;

INSERT INTO btree DEFAULT VALUES;
SELECT insert(1, null) FROM generate_series(1,17);

COMMIT;
