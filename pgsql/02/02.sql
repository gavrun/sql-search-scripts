DO $$
DECLARE
    search_uuid UUID; 
    search_cursor CURSOR FOR SELECT uuid FROM (VALUES 
        ('00000000-0000-0000-0000-000000000000'::UUID)
        --,('add multiple'::UUID)
        --,('YOUR_SEARCH_STARTS_HERE'::UUID)
    ) AS uuids(uuid);

    table_to_search RECORD;
    query TEXT;
    rows_found INT;
    table_exists BOOLEAN;
    
    table_query CURSOR FOR
        SELECT c.table_name, c.column_name, c.data_type 
        FROM information_schema.columns c
        JOIN information_schema.tables t
            ON c.table_name = t.table_name AND c.table_schema = t.table_schema
        WHERE t.table_type = 'BASE TABLE' 
        AND c.data_type IN ('uuid', 'character varying', 'text', 'char')
        AND c.table_schema = 'public';
BEGIN

    SELECT EXISTS (
        SELECT 1 FROM pg_tables WHERE tablename = 'searchresults'
    ) INTO table_exists;

    IF table_exists THEN
        EXECUTE 'TRUNCATE TABLE searchresults';
    ELSE
        EXECUTE '
            CREATE TEMPORARY TABLE searchresults (
                tablename TEXT,
                searchquery TEXT
            ) --ON COMMIT DROP
        ';
    END IF;

    OPEN search_cursor;
    LOOP
        FETCH search_cursor INTO search_uuid;
        EXIT WHEN NOT FOUND;
        
        OPEN table_query;
        LOOP
            FETCH table_query INTO table_to_search;
            EXIT WHEN NOT FOUND;
            
            IF table_to_search.data_type = 'uuid' THEN
                query := 'SELECT COUNT(*) FROM ' || quote_ident(table_to_search.table_name) || 
                         ' WHERE ' || quote_ident(table_to_search.column_name) || 
                         ' = ' || quote_literal(search_uuid);
            ELSE
                query := 'SELECT COUNT(*) FROM ' || quote_ident(table_to_search.table_name) || 
                         ' WHERE LOWER(' || quote_ident(table_to_search.column_name) || 
                         ') LIKE LOWER(' || quote_literal('%' || search_uuid || '%') || ')';
            END IF;
            
            BEGIN
                EXECUTE query INTO rows_found;
            EXCEPTION WHEN OTHERS THEN
                rows_found := 0;
            END;
 
            IF rows_found > 0 THEN
                INSERT INTO searchresults (tablename, searchquery)
                VALUES (
                    table_to_search.table_name,
                    CASE 
                        WHEN table_to_search.data_type = 'uuid' THEN
                            'SELECT * FROM ' || quote_ident(table_to_search.table_name) || 
                            ' WHERE ' || quote_ident(table_to_search.column_name) || 
                            ' = ' || quote_literal(search_uuid)
                        ELSE
                            'SELECT * FROM ' || quote_ident(table_to_search.table_name) || 
                            ' WHERE LOWER(' || quote_ident(table_to_search.column_name) || 
                            ') LIKE LOWER(' || quote_literal('%' || search_uuid || '%') || ')'
                    END
                );
            END IF;
            
        END LOOP;
        CLOSE table_query;
    END LOOP;
    CLOSE search_cursor;
END $$;

-- Check results
SELECT * FROM searchresults ORDER BY tablename;
