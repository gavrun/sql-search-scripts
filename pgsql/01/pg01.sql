-- Function searches all tables in the database 
CREATE OR REPLACE FUNCTION search_db(_like_pattern text)
  RETURNS TABLE(_tbl regclass, _ctid tid) AS
$func$
BEGIN
   FOR _tbl IN
      SELECT c.oid::regclass
      FROM   pg_class c
      JOIN   pg_namespace n ON n.oid = relnamespace
      WHERE  c.relkind = 'r'                           – only tables
      AND    n.nspname !~ '^(pg_|information_schema)'  -- exclude system schemas
      ORDER BY n.nspname, c.relname
   LOOP
      RETURN QUERY EXECUTE format(
         'SELECT $1, ctid FROM %s t WHERE t::text ~~ %L' , _tbl, '%' || _like_pattern || '%')
      USING _tbl;
   END LOOP;
END
$func$  LANGUAGE plpgsql

-- Usage:
SELECT * FROM search_db('%YOUR_SEARCH_STARTS_HERE%');
