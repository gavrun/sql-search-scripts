/* 
PostgreSQL search script 

This script searches for a specific text pattern in all rows of all base tables in the 'public' schema.
e.g., a string as uudi 
or string like int '12003'
or string like citext 'Backup Job' 
or string like timestamp '2024-08-01' introduces issues with time formats for different locales
or string like xml '<LatestRun>2024-07-26'

It works by serializing each row into JSONB and casting it to TEXT for a pattern matching. Focus here 
is getting matches as fast as possible without handling to much of metadata.

Results do not provide exact reference key. But often times example query works even without editing rhe identifier.

    Table Name, 'public."backup.model.backups"'	
    Match Count, '6'	
    Example Query, 'SELECT * FROM public."backup.model.backups" WHERE id = '80534610-b877-4c08-a2e6-44901121a254';'

Enable verbose output messages to follow which tables take most of the execution time.

Reminder. If script fails, manually run DROP TABLE search_database_results; 
*/

-- Clean up previous temp data (ensures idempotency)
DO $$
BEGIN
  DROP TABLE IF EXISTS search_database_results;
  RAISE NOTICE 'cleaned up temporary data';
END $$;

-- Create temporary table explicitly
CREATE TEMP TABLE search_database_results (
  found_in TEXT, 
  potential_key_value TEXT, -- Stores the value of the 'first' key in JSONB as text
  table_row_json JSONB
);
DO $$ BEGIN RAISE NOTICE 'temporary table "search_database_results" created'; END $$;

-- Loop through all tables and insert matching rows
DO $$
DECLARE
  enable_verbose_output BOOLEAN := true; -- Disable messages during execution
  search_pattern TEXT := '%YOUR_SEARCH_STARTS_HERE%'; -- Search pattern literal %SEARCH_SUB_STRING%
  rec RECORD;
  dyn_insert_sql TEXT;
  rows_inserted INTEGER;
  current_table_name TEXT;
BEGIN
  RAISE NOTICE 'searching by pattern: %', search_pattern;
  IF enable_verbose_output THEN
      RAISE NOTICE 'verbose output enabled';
  ELSE
      RAISE NOTICE 'verbose output disabled';
  END IF;
  
  -- Query information_schema to get table details.
  FOR rec IN 
    SELECT 
      table_schema, table_name
    FROM information_schema.tables 
    WHERE 
      table_schema = 'public' 
      AND table_type = 'BASE TABLE' 
    ORDER BY table_schema, table_name
  LOOP
    -- Format table name for display 
    current_table_name := format('%I.%I', rec.table_schema, rec.table_name);
    
    IF enable_verbose_output THEN
      RAISE NOTICE 'processing table: %', current_table_name;
    END IF;

    -- Dynamically construct the SQL INSERT statement using format()
    BEGIN
      dyn_insert_sql := format(
        'INSERT INTO search_database_results (found_in, potential_key_value, table_row_json) ' ||
        'SELECT %L, ' || -- Table name literal
        -- Extract the value of the first key found at the top level '$.*'
        '       jsonb_path_query_first(to_jsonb(t), ''$.*'') #>> ''{}'', ' || -- Order is based on JSON internal representation, often matches column order, but not guaranteed
        '       to_jsonb(t) ' || -- Full JSONB 
        'FROM %I.%I AS t ' ||
        'WHERE to_jsonb(t)::text LIKE %L',
        current_table_name, 
        rec.table_schema,   -- Schema identifier for FROM
        rec.table_name,     -- Table identifier for FROM
        search_pattern      -- Search pattern literal
      );
      
      -- IF enable_verbose_output THEN 
      --   RAISE NOTICE 'executing SQL: %', dyn_insert_sql; 
      -- END IF;
      
      -- Execute the dynamically generated SQL
      EXECUTE dyn_insert_sql;
      GET DIAGNOSTICS rows_inserted = ROW_COUNT;
      
      IF rows_inserted > 0 THEN
          RAISE NOTICE '>> found % match(es) in table %', rows_inserted, current_table_name;
      END IF;

    EXCEPTION -- Handle errors
      WHEN OTHERS THEN
        RAISE WARNING 'could not process table %. ERR: % (%)', current_table_name, SQLERRM, SQLSTATE;
    END;
  END LOOP;
  
  RAISE NOTICE 'search completed';
END $$;

-- Show all matching rows with their source table (pgAdmin only shows last statement)
SELECT 
  found_in, 
  potential_key_value, -- Value of the 'first' element in JSON row
  table_row_json 
FROM search_database_results
ORDER BY found_in,table_row_json;

-- Show unique tables and count of matches per table
SELECT 
  found_in AS "Table Name", 
  COUNT(*) AS "Match Count",
  CONCAT('SELECT * FROM ', found_in, ' WHERE id = ''',MIN(potential_key_value),''';') AS "Example Query"
FROM search_database_results
GROUP BY found_in
ORDER BY "Table Name";

-- Explicitly drop the temp table 
-- DROP TABLE search_database_results;
