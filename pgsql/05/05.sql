/* 
    PostgreSQL search script

    This script searches for a specific text in all rows of all base tables in the 'public' schema.
    e.g., a string as uudi 
    or string like int '12003'
    or string like citext 'Backup Job 1' 
    or string like timestamp '2024-08-01 18:39:56.537' 
    or string like xml '<LatestRun>2024-07-26T13:46:33.3158198+02:00</LatestRun>'
    
    '%%YOUR_SEARCH_STRING%%' uses %% escaped LIKE syntax for searching substrings
    '%%SUB_STRING_1%%SUB_STRING_2%%' syntax also works

    It works by serializing each row into JSONB, then converting it to XML, and searching through 
    the XML structure.

    1. Searches for text in all base tables.
    2. Saves results in a temporary table in the current session.
    3. Outputs matches and aggregated counts.
    4. Drops temp table.

    Output:
    tablename, 'public."backuprepositories.archivesettings"'
    query, 'SELECT * FROM public."backuprepositories.archivesettings";'
    table_row, '{"id": "f7317b15-1186-4a8b-9c7a-f4af97649209", "archive_schedule_options" : "<ScheduleOptions... }

    Reminder. If script fails, manually run DROP TABLE search_database_results; 
*/

-- Search for specific value in all the public tables, find matches, and store them to temp table
WITH
    found_rows AS (
    SELECT
        format('%I.%I', table_schema, table_name) AS table_name,
        query_to_xml(
      format(
        'SELECT to_jsonb(t) AS table_row 
         FROM %I.%I AS t 
         WHERE t::text LIKE ''%%YOUR_SEARCH_STARTS_HERE%%'' ', -- '%%SEARCH_STRING%%' keep %% escaped
        table_schema, 
        table_name
      ),
      true,   -- data-only
      false,  -- exclude nulls
      ''      -- default encoding
    ) AS table_rows
    FROM information_schema.tables
    WHERE 
    table_schema = 'public'
        AND table_type = 'BASE TABLE'
    )
SELECT
    table_name AS tablename,
    CONCAT('SELECT * FROM ', table_name, ';') AS query,
    x.table_row
INTO TEMP TABLE search_database_results
FROM found_rows AS f
LEFT JOIN xmltable(
  '//table/row' 
  PASSING f.table_rows
  COLUMNS table_row TEXT PATH 'table_row'
) AS x ON true
WHERE 
    x.table_row IS NOT NULL;

-- Show all matching rows with their source table 
SELECT * FROM search_database_results;

-- Show unique tables and count of matches per table 
SELECT
    query,
    COUNT(query) AS match_count
FROM search_database_results
GROUP BY query
ORDER BY query;

-- Clean up previous temp data 
DROP TABLE search_database_results;
