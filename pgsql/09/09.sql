-- ==========================================================================
-- PL/pgSQL search script
-- ==========================================================================

DO $$
DECLARE
    -- ======================================================================
    -- Configuration and Variables
    -- ======================================================================
    
    search_string TEXT := '%YOUR_SEARCH_STARTS_HERE%'; -- SEARCH STRING
    /* 'UUID' uses exact match,
       'STRING' without % or '_' wildcards uses exact match,
       '%SUBSTRING%' uses LIKE syntax for searching substrings,
       'NULL' or '' empty not supported */
    
    -- Filters 
    schema_name_include_filter TEXT[] := ARRAY['public']; -- List of schema names 
    schema_name_exclude_filter TEXT[] := NULL;
    /* NULL; uses all non-system schemas,
       ARRAY['public']; uses only public schema */
    table_name_include_filter TEXT[] := NULL;  
    table_name_exclude_filter TEXT[] := ARRAY['sessionlog','%sess%'];  -- List of table names
    /* NULL; disables filtering,
       ARRAY['sessionlog']; excludes only sessionlog table,
       ARRAY['%sess%']; excludes tables by name pattern
       ARRAY['sessionlog','%sess%']; excludes sessionlog table and other tables by pattern */
    column_name_include_filter TEXT[] := NULL;
    column_name_exclude_filter TEXT[] := NULL; -- List of column names 

    -- Search scope
    include_uuid BOOLEAN := TRUE; -- uuid 
    include_text BOOLEAN := TRUE; -- text, varchar, char, bpchar, citext, name
    include_numeric BOOLEAN := TRUE; -- int, numeric, float, serials, money etc.
    include_datetime BOOLEAN := TRUE; -- timestamp, date, time, interval
    include_xml BOOLEAN := TRUE; -- xml
    include_json BOOLEAN := TRUE; -- json, jsonb

    -- Detailed debug messages 
    debug_mode BOOLEAN := TRUE; -->> TRUE FALSE

    -- Helper variables
    max_string_preview_length INT := 128; -- max length of value preview in results
    start_time_overall TIMESTAMPTZ; 
    end_time_overall TIMESTAMPTZ;
    start_time_init TIMESTAMPTZ; 
    end_time_init TIMESTAMPTZ;
    start_time_meta TIMESTAMPTZ; 
    end_time_meta TIMESTAMPTZ;
    start_time_search TIMESTAMPTZ; 
    end_time_search TIMESTAMPTZ;
    start_time_query_gen TIMESTAMPTZ; 
    end_time_query_gen TIMESTAMPTZ;
    current_client_min_messages TEXT;
    sql_query TEXT;
    table_rec RECORD;
    column_rec RECORD;
    pk_rec RECORD;
    intermediate_rec RECORD;
    pk_info_rec RECORD;
    pk_json_expression TEXT;
    pk_where_clause TEXT;
    pk_col_value TEXT;
    generated_query TEXT;
    error_message TEXT;
    rows_affected INT;
    current_table_id INT;
    current_column_id INT;
    current_schema_name TEXT;
    current_table_name TEXT;
    current_quoted_table_name TEXT;
    current_column_name TEXT;
    current_quoted_column_name TEXT;
    current_data_type TEXT;
    current_udt_name TEXT;
    current_search_expression TEXT;
    current_select_expression TEXT;
    pk_col_name TEXT;
    quoted_pk_col_name_in_loop TEXT;
    pk_col_data_type TEXT;
    fallback_table_name TEXT;
    fallback_column_name TEXT;
    fallback_column_type TEXT;
    fallback_column_value TEXT;
    search_expression_for_fallback TEXT;

BEGIN
    -- ======================================================================
    -- Initialization
    -- ======================================================================
    
    RAISE NOTICE 'INFO: Script execution started.';
    start_time_overall := clock_timestamp();
    start_time_init := clock_timestamp();

    -- Get current client_min_messages setting
    SELECT setting INTO current_client_min_messages FROM pg_settings WHERE name = 'client_min_messages';
    RAISE NOTICE 'INFO: Current client_min_messages: %', current_client_min_messages;
    IF debug_mode AND current_client_min_messages <> 'debug' AND current_client_min_messages <> 'log' THEN
      RAISE NOTICE 'INFO: Set client_min_messages = debug for more detailed DEBUG messages.';
    END IF;

    IF debug_mode THEN RAISE NOTICE 'DEBUG: Initializing...'; END IF;

    -- Drop temporary tables if they exist from a previous run 
    DROP TABLE IF EXISTS temp_results;
    DROP TABLE IF EXISTS temp_table_metadata;
    DROP TABLE IF EXISTS temp_column_metadata;
    DROP TABLE IF EXISTS temp_primary_key_info;
    DROP TABLE IF EXISTS temp_intermediate_results;

    -- Create temporary tables

    CREATE TEMPORARY TABLE temp_results (
        result_id SERIAL PRIMARY KEY,
        table_name TEXT,
        column_name TEXT,
        column_value TEXT,
        column_type TEXT,
        value_query TEXT,
        error_message TEXT NULL
    );

    CREATE TEMPORARY TABLE temp_table_metadata (
        table_id SERIAL PRIMARY KEY,
        schema_name TEXT,
        table_name TEXT,
        quoted_table_name TEXT
    );

    CREATE TEMPORARY TABLE temp_column_metadata (
        column_id SERIAL PRIMARY KEY,
        table_id INT,
        column_name TEXT,
        quoted_column_name TEXT,
        data_type TEXT,
        udt_name TEXT,
        is_searchable BOOLEAN,
        search_expression TEXT,
        select_expression TEXT
    );

    CREATE TEMPORARY TABLE temp_primary_key_info (
        pk_info_id SERIAL PRIMARY KEY,
        table_id INT,
        pk_column_name TEXT,
        quoted_pk_column_name TEXT,
        pk_column_data_type TEXT,
        ordinal_position INT
    );

    CREATE TEMPORARY TABLE temp_intermediate_results (
        intermediate_id SERIAL PRIMARY KEY,
        table_id INT,
        column_id INT,
        found_value TEXT,
        pk_values_json JSONB NULL
    );

    -- Record section
    end_time_init := clock_timestamp();
    IF debug_mode THEN RAISE NOTICE 'DEBUG: Initialization completed in % ms',
        EXTRACT(MILLISECOND FROM (end_time_init - start_time_init)); END IF;

    -- ======================================================================
    -- Metadata Gathering
    -- ======================================================================

    start_time_meta := clock_timestamp();
    RAISE NOTICE 'DEBUG: Gathering metadata...';

    -- Populate table metadata with tables matching filters
    INSERT INTO temp_table_metadata (schema_name, table_name, quoted_table_name)
    SELECT
        t.table_schema,
        t.table_name,
        format('%I.%I', t.table_schema, t.table_name) -- Format safely with quotes for dynamic SQL
    FROM information_schema.tables t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema NOT IN ('pg_catalog', 'information_schema') -- Exclude system and toast schemas
      AND t.table_schema <> 'pg_toast'
    --   AND (schema_name_include_filter IS NULL OR t.table_schema LIKE schema_name_include_filter) -- Apply filters
    --   AND (schema_name_exclude_filter IS NULL OR t.table_schema NOT LIKE schema_name_exclude_filter)
    --   AND (table_name_include_filter IS NULL OR t.table_name LIKE table_name_include_filter)
    --   AND (table_name_exclude_filter IS NULL OR t.table_name NOT LIKE table_name_exclude_filter)
    AND (schema_name_include_filter IS NULL OR 
        EXISTS (SELECT 1 FROM unnest(schema_name_include_filter) AS p(pattern) -- unnest array into rows with 'pattern' column
                WHERE t.table_schema LIKE p.pattern) -- LIKE against each element
        )
    AND (schema_name_exclude_filter IS NULL OR
            NOT EXISTS (SELECT 1 FROM unnest(schema_name_exclude_filter) AS p(pattern)
                        WHERE t.table_schema LIKE p.pattern)
        )
    AND (table_name_include_filter IS NULL OR
            EXISTS (SELECT 1 FROM unnest(table_name_include_filter) AS p(pattern)
                    WHERE t.table_name LIKE p.pattern)
        )
    AND (table_name_exclude_filter IS NULL OR
            NOT EXISTS (SELECT 1 FROM unnest(table_name_exclude_filter) AS p(pattern)
                        WHERE t.table_name LIKE p.pattern)
        );

    -- Get number of tables found
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    RAISE NOTICE 'DEBUG: Found % tables matching filters.', rows_affected;

    IF debug_mode THEN RAISE NOTICE 'DEBUG: Gathering column and primary key metadata...'; END IF;

    -- Iterate through each identified table to get column and PK info
    FOR table_rec IN SELECT table_id, schema_name, table_name, quoted_table_name FROM temp_table_metadata LOOP
        current_table_id := table_rec.table_id;
        current_quoted_table_name := table_rec.quoted_table_name; 
        IF debug_mode THEN RAISE NOTICE 'DEBUG: Processing table: %', current_quoted_table_name; END IF;

        -- Gather columns for the current table and insert into column metadata temp table
        INSERT INTO temp_column_metadata 
            (table_id, column_name, quoted_column_name, data_type, udt_name, is_searchable, search_expression, select_expression)
        SELECT
            current_table_id,
            c.column_name,
            format('%I', c.column_name),
            c.data_type, -- Base data type
            c.udt_name,  -- User-defined type name (e.g. citext)
            CASE
                -- Apply 'searchable' config flags and by types (data_type + udt_name)
                WHEN include_uuid AND c.udt_name = 'uuid' THEN TRUE
                WHEN include_text AND (c.udt_name = 'citext' OR 
                    c.data_type IN ('character varying', 'varchar', 'character', 'char', 'text', 'name', 'bpchar')) THEN TRUE
                WHEN include_numeric AND c.data_type 
                    IN ('smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision', 'smallserial', 'serial', 'bigserial', 'money') 
                    THEN TRUE
                WHEN include_datetime AND c.data_type 
                    IN ('timestamp without time zone', 'timestamp with time zone', 'date', 'time without time zone', 'time with time zone', 'interval') 
                    THEN TRUE
                WHEN include_xml AND c.data_type = 'xml' THEN TRUE
                WHEN include_json AND c.data_type IN ('json', 'jsonb') THEN TRUE
                ELSE FALSE
            END,
            CASE
                -- Determine SQL expression for WHERE clause based on type 
                WHEN c.udt_name = 'citext' THEN format('%I', c.column_name)
                WHEN c.data_type = 'xml' THEN format('xmlserialize(content %I as text)', c.column_name)
                ELSE format('%I::text', c.column_name)
            END,
            CASE
                -- Determine SQL expression for selecting the preview value based on type
                WHEN c.udt_name = 'citext' THEN format('left(%I, %s)', c.column_name, max_string_preview_length)
                WHEN c.data_type = 'xml' THEN format('left(xmlserialize(content %I as text), %s)', c.column_name, max_string_preview_length)
                ELSE format('left(%I::text, %s)', c.column_name, max_string_preview_length)
            END
        FROM information_schema.columns c
        WHERE c.table_schema = table_rec.schema_name
          AND c.table_name = table_rec.table_name
        --   AND (column_name_include_filter IS NULL OR c.column_name LIKE column_name_include_filter)  -- Apply filters
        --   AND (column_name_exclude_filter IS NULL OR c.column_name NOT LIKE column_name_exclude_filter)
        AND (column_name_include_filter IS NULL OR
            EXISTS (SELECT 1 FROM unnest(column_name_include_filter) AS p(pattern)
                    WHERE c.column_name LIKE p.pattern)
        )
        AND (column_name_exclude_filter IS NULL OR
            NOT EXISTS (SELECT 1 FROM unnest(column_name_exclude_filter) AS p(pattern)
                        WHERE c.column_name LIKE p.pattern)
        );

        -- Gather primary key columns for the current table
        INSERT INTO temp_primary_key_info (table_id, pk_column_name, quoted_pk_column_name, pk_column_data_type, ordinal_position)
        SELECT
            current_table_id,
            kcu.column_name,
            format('%I', kcu.column_name), -- Format PK column name
            col.data_type,
            kcu.ordinal_position -- Position for multi-column PKs
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc -- Find constraints of PRIMARY KEY
            ON kcu.constraint_name = tc.constraint_name
            AND kcu.table_schema = tc.table_schema
            AND kcu.table_name = tc.table_name
        JOIN information_schema.columns col -- Get the data type of the PK column
            ON kcu.table_schema = col.table_schema
            AND kcu.table_name = col.table_name
            AND kcu.column_name = col.column_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND kcu.table_schema = table_rec.schema_name
          AND kcu.table_name = table_rec.table_name
        ORDER BY kcu.ordinal_position; -- Order for multi-column PKs!

    END LOOP;

    end_time_meta := clock_timestamp();
    RAISE NOTICE 'DEBUG: Metadata gathering completed in % ms', EXTRACT(MILLISECOND FROM (end_time_meta - start_time_meta));

    -- ======================================================================
    -- Search Execution
    -- ======================================================================

    start_time_search := clock_timestamp();
    RAISE NOTICE 'DEBUG: Starting search execution...';

    -- Iterate through each column marked as searchable in the metadata
    FOR column_rec IN
        SELECT
            c.column_id, t.schema_name, t.table_name, t.quoted_table_name, c.column_name, c.quoted_column_name, 
            c.data_type, c.udt_name, c.search_expression, c.select_expression, c.table_id
        FROM temp_column_metadata c
        JOIN temp_table_metadata t ON c.table_id = t.table_id
        WHERE c.is_searchable = TRUE -- Flagged here
    LOOP
        -- Store details from the loop record
        current_table_id := column_rec.table_id;
        current_column_id := column_rec.column_id;
        current_quoted_table_name := column_rec.quoted_table_name;
        current_quoted_column_name := column_rec.quoted_column_name;
        current_search_expression := column_rec.search_expression; -- "col"::text or "col" for citext
        current_select_expression := column_rec.select_expression; -- left("col"::text, N)
        current_data_type := column_rec.data_type;

        IF debug_mode THEN RAISE NOTICE 'DEBUG: Searching Table: %, Column: %', current_quoted_table_name, current_quoted_column_name; END IF;

        -- Prepare the expression to capture primary key values as JSONB for the current table
        pk_json_expression := 'NULL::jsonb';
        SELECT string_agg(format('%L, %I::text', pk_column_name, pk_column_name), ', ')
        INTO pk_json_expression
        FROM temp_primary_key_info
        WHERE table_id = current_table_id;

        -- Build the jsonb_build_object expression if PK columns found
        IF pk_json_expression IS NOT NULL THEN
            pk_json_expression := 'jsonb_build_object(' || pk_json_expression || ')';
        ELSE
            pk_json_expression := 'NULL::jsonb';
        END IF;

        -- Build the dynamic SQL query to search the current column and insert results
        sql_query := format(
            'INSERT INTO temp_intermediate_results (table_id, column_id, found_value, pk_values_json) ' ||
            'SELECT %s, %s, %s, %s FROM %s WHERE %s LIKE %L',
            current_table_id,
            current_column_id,
            current_select_expression,
            pk_json_expression, -- jsonb_build_object(...) or NULL::jsonb
            current_quoted_table_name,
            current_search_expression,
            search_string -- Search string literal
        );

        IF debug_mode THEN RAISE NOTICE 'DEBUG: Executing Search SQL: %', sql_query; END IF;

        -- Execute the dynamic SQL search query 
        BEGIN
            EXECUTE sql_query;
            GET DIAGNOSTICS rows_affected = ROW_COUNT;
            IF debug_mode THEN RAISE NOTICE 'DEBUG: Found % matches.', rows_affected; END IF;
        EXCEPTION -- Handle errors
            WHEN others THEN
            GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
            RAISE WARNING 'Failed to search Table: %, Column: %. Error: %', current_quoted_table_name, current_quoted_column_name, error_message;
            INSERT INTO temp_results (table_name, column_name, column_type, error_message)
            VALUES (current_quoted_table_name, current_quoted_column_name, current_data_type, 'Search failed: ' || error_message);
        END;

    END LOOP;

    end_time_search := clock_timestamp();
    RAISE NOTICE 'DEBUG: Search execution completed in % ms', EXTRACT(MILLISECOND FROM (end_time_search - start_time_search));

    -- ======================================================================
    -- Query Generation
    -- ======================================================================

    start_time_query_gen := clock_timestamp();
    RAISE NOTICE 'DEBUG: Starting query generation...';

    -- Iterate through each item found during the search execution 
    FOR intermediate_rec IN
        SELECT ir.intermediate_id, ir.table_id, ir.column_id, ir.found_value, ir.pk_values_json,
               t.quoted_table_name, c.quoted_column_name, 
               c.data_type, c.udt_name, -- Include data_type and udt_name!
               c.search_expression
        FROM temp_intermediate_results ir
        JOIN temp_table_metadata t ON ir.table_id = t.table_id
        JOIN temp_column_metadata c ON ir.column_id = c.column_id
    LOOP
        -- Variables for each intermediate result row
        generated_query := NULL;
        error_message := NULL;
        pk_where_clause := NULL;

        BEGIN
            -- Attempt to generate query using Primary Key 
            IF intermediate_rec.pk_values_json IS NOT NULL THEN
                -- Check each column in the primary key for this table
                FOR pk_info_rec IN
                    SELECT pki.pk_column_name, pki.quoted_pk_column_name, pki.pk_column_data_type
                    FROM temp_primary_key_info pki
                    WHERE pki.table_id = intermediate_rec.table_id
                    ORDER BY pki.ordinal_position -- Order!
                LOOP
                    -- Extract PK column name and value from JSON
                    pk_col_name := pk_info_rec.pk_column_name;
                    quoted_pk_col_name_in_loop := pk_info_rec.quoted_pk_column_name;
                    pk_col_data_type := pk_info_rec.pk_column_data_type;
                    pk_col_value := intermediate_rec.pk_values_json ->> pk_col_name;

                    -- Build WHERE clause
                    IF pk_col_value IS NOT NULL THEN
                         pk_where_clause := concat_ws(' AND ', pk_where_clause, format('%I = %L', quoted_pk_col_name_in_loop, pk_col_value));
                    ELSE
                         pk_where_clause := concat_ws(' AND ', pk_where_clause, format('%I IS NULL', quoted_pk_col_name_in_loop));
                    END IF;
                END LOOP;

                -- Generate final SELECT query if WHERE clause was built
                IF pk_where_clause IS NOT NULL THEN
                    generated_query := format('SELECT * FROM %s WHERE %s;', intermediate_rec.quoted_table_name, pk_where_clause);
                ELSE
                    error_message := 'Failed to build PK WHERE clause (PK JSON might be empty or PK info missing).';
                END IF;
            END IF;

            -- Check if query generation based on PK failed or was not possible (no PK)
            IF generated_query IS NULL 
            THEN
                -- Fallback logic to generate a less specific query
                -- Variables with details for fallback
                fallback_table_name := intermediate_rec.quoted_table_name;
                fallback_column_name := intermediate_rec.quoted_column_name;
                fallback_column_type := intermediate_rec.data_type; -- Base type matters!
                fallback_column_value := intermediate_rec.found_value; -- Preview value
                search_expression_for_fallback := intermediate_rec.search_expression; -- Search literal

                -- Select the fallback strategy based on data type and search literal (important!)
                IF fallback_column_type = 'uuid' OR
                   (fallback_column_type IN ('character varying', 'varchar', 'character', 'char', 'text', 'name', 'citext', 'bpchar')
                    AND position('%' in search_string) = 0 
                    AND position('_' in search_string) = 0 -- literal has wildcards?
                    --AND length(fallback_column_value) <= 50 -- literal is long ?
                    )
                THEN
                    -- Fallback 1: Use the exact matched value e.g. UUIDs (WHERE column = value)
                    IF debug_mode THEN RAISE NOTICE 'DEBUG: Fallback (Matched Value): Table: %, Column: %', fallback_table_name, fallback_column_name; END IF;

                    -- Generate SELECT query 
                    generated_query := format('SELECT * FROM %s WHERE %I = %L; /* Fallback query (matched value) */', -- Add a hint to the query comment :-)
                                               fallback_table_name, fallback_column_name, fallback_column_value);
                    IF intermediate_rec.pk_values_json IS NULL 
                    THEN
                        error_message := coalesce(error_message || ' ', '') || 'No PK found for table; used matched value for query.';
                    ELSE
                        error_message := coalesce(error_message || ' ', '') || 'Used matched value for query (unexpected fallback with existing PK info).';
                    END IF;
                ELSE
                    -- Fallback 2: Use the original search criteria (WHERE column LIKE search_string)
                    IF debug_mode THEN RAISE NOTICE 'DEBUG: Fallback (Original Search): Table: %, Column: %', fallback_table_name, fallback_column_name; END IF;

                    generated_query := format('SELECT * FROM %s WHERE %s LIKE %L; /* Fallback query (original search) */',
                                               fallback_table_name, search_expression_for_fallback, search_string,
                                               replace(coalesce(fallback_column_value, 'NULL'), '*/', '* /'));
                    IF intermediate_rec.pk_values_json IS NOT NULL AND pk_where_clause IS NULL 
                    THEN
                         error_message := coalesce(error_message || ' ', '') || 'Used original search criteria as fallback (PK query generation failed).';
                    ELSIF intermediate_rec.pk_values_json IS NULL THEN
                         error_message := coalesce(error_message || ' ', '') || 'Cannot reliably generate specific row query (no PK/unsuitable value). Using original search criteria.';
                    ELSE
                         error_message := coalesce(error_message || ' ', '') || 'Used original search criteria as fallback.';
                    END IF;
                END IF;
            END IF;

        EXCEPTION -- Handle errors
            WHEN others THEN
             GET STACKED DIAGNOSTICS error_message = MESSAGE_TEXT;
             error_message := 'ERROR generating query: ' || error_message;
             IF debug_mode THEN RAISE NOTICE 'DEBUG: %', error_message; END IF;
        END;

        -- Insert the final result for this item into the results table
        INSERT INTO temp_results (table_name, column_name, column_value, column_type, value_query, error_message)
        VALUES (
            intermediate_rec.quoted_table_name,
            intermediate_rec.quoted_column_name,
            intermediate_rec.found_value, -- Preview value
            CASE
                -- Column type to display 
                WHEN intermediate_rec.data_type = 'USER-DEFINED' THEN intermediate_rec.udt_name
                ELSE intermediate_rec.data_type
            END,
            generated_query, -- Query
            error_message    -- Error or fallback info message
        );

    END LOOP;

    end_time_query_gen := clock_timestamp();
    RAISE NOTICE 'DEBUG: Query generation completed in % ms', EXTRACT(MILLISECOND FROM (end_time_query_gen - start_time_query_gen));

    -- ======================================================================
    -- Final Output
    -- ======================================================================

    -- Record overall 
    end_time_overall := clock_timestamp();
    RAISE NOTICE 'INFO: Script execution finished.';
    RAISE NOTICE 'DEBUG: Total script execution time: % ms', EXTRACT(MILLISECOND FROM (end_time_overall - start_time_overall));

    -- SELECT * FROM temp_results ORDER BY table_name, column_name;

    -- Explicit cleanup (temp tables are dropped automatically at session end)
    /*
    DROP TABLE IF EXISTS temp_results;
    DROP TABLE IF EXISTS temp_table_metadata;
    DROP TABLE IF EXISTS temp_column_metadata;
    DROP TABLE IF EXISTS temp_primary_key_info;
    DROP TABLE IF EXISTS temp_intermediate_results;
    */

END $$;

-- See the Final Output results:
SELECT * FROM temp_results ORDER BY table_name, column_name;

--END
-- ==========================================================================