/* PostgreSQL search function for TEXT matching */

-- SELECT * FROM search_database('%YOUR_SEARCH_STARTS_HERE%');

-- See usage examples after definition

CREATE OR REPLACE FUNCTION search_database(
    search_value TEXT,                     -- Text pattern to search (uses ILIKE syntax)
    schema_filter TEXT DEFAULT 'public',   -- LIKE pattern for schema names (e.g., 'public', 'app_%', '%')
    table_filter TEXT DEFAULT '%',         -- LIKE pattern for table names
    column_filter TEXT DEFAULT '%',        -- LIKE pattern for column names
    include_text BOOLEAN DEFAULT true,     -- Search in text-based types (text, varchar, char, name, citext, etc.)
    include_numeric BOOLEAN DEFAULT true,  -- Search in numeric types (int, numeric, float, etc.)
    include_xml BOOLEAN DEFAULT true,      -- Search in 'xml' type columns
    include_json BOOLEAN DEFAULT true,     -- Search in 'json' and 'jsonb' type columns
    include_datetime BOOLEAN DEFAULT true  -- Search in date/time types (timestamp, date, time, interval)
                                           -- UUID type is not supported deliberately
)

RETURNS TABLE ( 
    result_schema_name TEXT,
    result_table_name TEXT,
    result_column_name TEXT,
    matched_value_snippet TEXT, -- Snippet of the found value 
    result_column_type TEXT,    
    match_count BIGINT          -- How many rows had this specific snippet in this column
)

AS $$

DECLARE

    tbl RECORD; -- Schema and table info (raw names for querying, quoted names for messages)
    col RECORD; -- Column info (raw column name, quoted name, actual type name from pg_type.typname)

    -- Dynamically generated SQL
    sql_command TEXT;
    where_clause TEXT;                 -- Generated WHERE clause (e.g., '"col"::text ILIKE ''%val%''')
    select_snippet_expr TEXT;          -- Selecting the snippet (e.g., 'LEFT("col"::text, 128)')
    format_style TEXT;                 
    like_pattern TEXT := search_value; -- Search pattern (passed directly)
    match_count_in_col BIGINT;         

BEGIN 
    -- Temporary table to store match details automatically dropped at the end of the transaction/session
    CREATE TEMP TABLE temp_results (
        schema_name TEXT,
        table_name TEXT,
        column_name TEXT,
        column_value TEXT,
        column_type TEXT
    ) ON COMMIT DROP;

    -- Loop through schemas and tables [information_schema.tables]
    RAISE DEBUG 'Starting table loop. Schema filter: %, Table filter: %', schema_filter, table_filter;
    
    -- Outer loop iterating through tables
    FOR tbl IN 
        SELECT
            table_schema as raw_schema,   -- Raw schema name for querying pg_catalog and for format() %I
            table_name as raw_table,     -- Raw table name for querying pg_catalog and for format() %I
            quote_ident(table_schema) as quoted_schema, 
            quote_ident(table_name) as quoted_table 
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' -- Only search actual tables (not views, etc.)
          AND table_schema LIKE schema_filter 
          AND table_schema NOT IN ('pg_catalog', 'information_schema') -- Exclude system schemas
          AND table_name LIKE table_filter -- Apply table name filter
    LOOP 
        RAISE DEBUG 'Processing table: %.%', tbl.quoted_schema, tbl.quoted_table;

        -- Inner loop iterate through columns [pg_catalog], [pg_type]
        -- [information_schema] reports 'citext' as 'text'
        FOR col IN 
            SELECT
                a.attname AS raw_column, -- Raw column name (for format() %I)
                quote_ident(a.attname) AS quoted_column, 
                t.typname AS data_type   -- Type name (e.g., 'citext', 'varchar', 'int4')
            FROM
                pg_attribute a 
            JOIN
                pg_class c ON a.attrelid = c.oid 
            JOIN
                pg_namespace n ON c.relnamespace = n.oid 
            JOIN
                pg_type t ON a.atttypid = t.oid 
            WHERE
                n.nspname = tbl.raw_schema          
                AND c.relname = tbl.raw_table       
                AND c.relkind = 'r'      -- Ensure it's a regular table ('r') not an index, view, etc.
                AND a.attnum > 0         -- Exclude system columns (like oid, ctid, xmin, etc.)
                AND NOT a.attisdropped 
                AND a.attname LIKE column_filter 
                -- Filter by data types based on the include_* flags, using the accurate t.typname:
                AND (
                      -- Text-based types
                      (include_text AND t.typname IN ('text', 'varchar', 'bpchar', 'char', 'name', 'citext'))
                      -- Numeric types
                      OR (include_numeric AND t.typname IN ('int2', 'int4', 'int8', 'numeric', 'float4', 'float8'))
                      -- XML type
                      OR (include_xml AND t.typname = 'xml')
                      -- JSON types
                      OR (include_json AND t.typname IN ('json', 'jsonb'))
                      -- Date/Time types
                      OR (include_datetime AND t.typname IN ('timestamp', 'timestamptz', 'date', 'time', 'timetz', 'interval'))
                    ) 
        LOOP 
            RAISE DEBUG '  Processing column: % (%)', col.quoted_column, col.data_type; 

            -- Construct WHERE clause and SELECT snippet expression based on the data type

            -- Specific types
            IF col.data_type = 'citext' THEN
                where_clause := format('%I ILIKE %L', col.raw_column, like_pattern); -- %I for identifier, %L for literal pattern
                select_snippet_expr := format('LEFT(%I::text, 128)', col.raw_column);
                RAISE DEBUG '    Using direct ILIKE for citext.';

            ELSIF include_datetime AND col.data_type IN ('timestamp', 'timestamptz', 'date', 'time', 'timetz') THEN
                -- Handle timestamp, date, and time types using to_char for locale-independent searching (ISO 8601 / ODBC style 121)
                CASE
                    WHEN col.data_type IN ('timestamp', 'timestamptz') THEN format_style := 'YYYY-MM-DD HH24:MI:SS.MS';
                    WHEN col.data_type = 'date' THEN format_style := 'YYYY-MM-DD';
                    WHEN col.data_type IN ('time', 'timetz') THEN format_style := 'HH24:MI:SS.MS';
                    ELSE format_style := 'YYYY-MM-DD HH24:MI:SS.MS'; --fallback
                END CASE;
                where_clause := format('to_char(%I, %L) ILIKE %L', col.raw_column, format_style, like_pattern);
                select_snippet_expr := format('LEFT(to_char(%I, %L), 128)', col.raw_column, format_style);
                RAISE DEBUG '    Using to_char(%L) with ILIKE for datetime type.', format_style;

            ELSIF include_datetime AND col.data_type = 'interval' THEN
                where_clause := format('%I::text ILIKE %L', col.raw_column, like_pattern);
                select_snippet_expr := format('LEFT(%I::text, 128)', col.raw_column);
                RAISE DEBUG '    Using ::text cast with ILIKE for interval type.';
            
            -- All other types (numeric, xml, json, standard text like 'text', 'varchar', etc.)
            ELSE 
                where_clause := format('%I::text ILIKE %L', col.raw_column, like_pattern);
                select_snippet_expr := format('LEFT(%I::text, 128)', col.raw_column);
                RAISE DEBUG '    Using ILIKE with ::text cast for other types (%s).', col.data_type;
            END IF; 

            -- Construct the dynamic SQL command to count matches 
            sql_command := format(
                'SELECT count(*) FROM %I.%I WHERE %s',
                tbl.raw_schema, tbl.raw_table,  -- FROM schema.table (%I for identifiers)
                where_clause                    -- WHERE (%s for where_clause)
            ); 

            BEGIN 
                -- Execute the count check to handle potential errors (e.g., permissions issues, data type incompatibilities, etc.)
                RAISE DEBUG '    Executing count check: %', sql_command; 
                EXECUTE sql_command INTO match_count_in_col;
                
                RAISE NOTICE '    Count for %.%.% (type: %) with pattern %L: %',
                             tbl.quoted_schema, tbl.quoted_table, col.quoted_column,
                             col.data_type,
                             like_pattern, match_count_in_col; 

                IF match_count_in_col > 0 THEN
                    RAISE NOTICE '    >>> Found % matches, inserting snippets...', match_count_in_col; 
                    
                    -- Construct the dynamic SQL command to insert the matching snippets
                    sql_command := format(
                        'INSERT INTO temp_results (schema_name, table_name, column_name, column_value, column_type)
                         SELECT %L, %L, %L, %s, %L
                         FROM %I.%I
                         WHERE %s',
                        tbl.raw_schema,         -- Literal schema name 
                        tbl.raw_table,          -- Literal table name 
                        col.raw_column,         -- Literal column name 
                        select_snippet_expr,    -- Snippet (%s for select_snippet_expr)
                        col.data_type,          -- Literal data type 
                        tbl.raw_schema,         -- Raw schema name for FROM clause (%I)
                        tbl.raw_table,          -- Raw table name for FROM clause (%I)
                        where_clause            -- WHERE (%s for where_clause)
                    ); 
                    RAISE DEBUG '      Executing insert: %', sql_command; 
                    EXECUTE sql_command; 

                    -- Check the FOUND variable after insert (should normally be true if count > 0 if not some transaction issues)
                    IF FOUND THEN
                       RAISE DEBUG '      FOUND is TRUE after INSERT (expected).'; 
                    ELSE
                       RAISE WARNING '      FOUND is FALSE after INSERT even though count was > 0!'; 
                    END IF; 
                END IF; 

            EXCEPTION -- Handle issues
                WHEN OTHERS THEN
                    RAISE WARNING 'Could not search column %.%.% (type: %): % (%)',
                        tbl.quoted_schema, tbl.quoted_table, col.quoted_column,
                        col.data_type,
                        SQLERRM, SQLSTATE;
            END; 

        END LOOP; 
    END LOOP; 

    RAISE DEBUG 'Loops complete. Aggregating results from temp_results.'; 

    -- Return the aggregated summary results from the temporary table
    RETURN QUERY 
    SELECT
        tr.schema_name::TEXT,
        tr.table_name::TEXT,
        tr.column_name::TEXT,
        tr.column_value::TEXT,
        tr.column_type::TEXT,
        COUNT(*)::BIGINT AS match_count
    FROM temp_results tr
    GROUP BY tr.schema_name, tr.table_name, tr.column_name, tr.column_value, tr.column_type
    ORDER BY tr.schema_name, tr.table_name, tr.column_name, tr.column_value; 

END; 

$$ LANGUAGE plpgsql; 

-- Configuration:

-- Set client_min_messages to NOTICE or DEBUG to see messages (option on the PostgreSQL session level)
-- SET client_min_messages = DEBUG;

-- Usage examples:

-- Search for '%Backup Job%' in all default column types (text, numeric, xml, json, datetime) 
-- SELECT * FROM search_database('%Backup Job%');

-- Search for a specific timestamp fragment
-- SELECT * FROM search_database('%2025-01-10 10:30%');

-- Search for '%value%' in all text and numeric columns ONLY (text, numeric)
-- SELECT * FROM search_database('%value%', 'public', '%', '%', true, true, false, false, false);

-- Cleanup

-- DROP FUNCTION IF EXISTS search_database(TEXT, TEXT, TEXT, TEXT, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN);
-- RESET client_min_messages;
