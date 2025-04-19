-- ==========================================================================
-- T-SQL search script
-- ==========================================================================

-- ==========================================================================
-- Configuration and Variables
-- ==========================================================================

/* Search: */
DECLARE @SearchString NVARCHAR(500) = '%YOUR_SEARCH_STARTS_HERE%' -- Value to SEARCH for 
/* '%YOUR_SEARCH_STRING%' uses LIKE syntax for searching substrings, 
   'YOUR_UUID' without % uses exact match for UUIDs */

DECLARE @TableNameFilter NVARCHAR(255) = NULL -- Limit SEARCH scope: Filter tables (LIKE syntax, NULL for all)
DECLARE @ColumnNameFilter NVARCHAR(255) = NULL -- Limit SEARCH scope: Filter columns (LIKE syntax, NULL for all)

DECLARE @IncludeNumeric BIT = 1 -- SEARCH numeric columns? (0=No, 1=Yes)
DECLARE @IncludeDateTime BIT = 1 -- SEARCH datetime-related columns? (0=No, 1=Yes)
DECLARE @IncludeXml BIT = 1 -- SEARCH XML columns? (0=No, 1=Yes)

DECLARE @MaxStringPreviewLength INT = 256 -- Max length of found value preview in results

/* Debug: */
DECLARE @DebugMode BIT = 1 -- Output DEBUG mode messages? (0=No, 1=Yes)
DECLARE @StartTime_Overall DATETIME2(3), @EndTime_Overall DATETIME2(3)
DECLARE @StartTime_Init DATETIME2(3), @EndTime_Init DATETIME2(3)
DECLARE @StartTime_Meta DATETIME2(3), @EndTime_Meta DATETIME2(3)
DECLARE @StartTime_Search DATETIME2(3), @EndTime_Search DATETIME2(3)
DECLARE @StartTime_QueryGen DATETIME2(3), @EndTime_QueryGen DATETIME2(3)
/* Debug: comment RETURN in CATCH blocks to avoid execution interruption of the entire script 
to allow the script to complete remaining phases for DEBUG output */

-- ==========================================================================
-- Initialization
-- ==========================================================================

SET NOCOUNT ON --Tturn off row count messages for cleaner output

IF @DebugMode = 1 
    SET @StartTime_Overall = SYSDATETIME()
    SET @StartTime_Init = SYSDATETIME()
    PRINT '-- Initializing... --' 

-- Drop temporary tables if they exist
IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results
IF OBJECT_ID('tempdb..#TableMetadata') IS NOT NULL DROP TABLE #TableMetadata
IF OBJECT_ID('tempdb..#ColumnMetadata') IS NOT NULL DROP TABLE #ColumnMetadata
IF OBJECT_ID('tempdb..#PrimaryKeyInfo') IS NOT NULL DROP TABLE #PrimaryKeyInfo
IF OBJECT_ID('tempdb..#IntermediateResults') IS NOT NULL DROP TABLE #IntermediateResults

-- Create the search results table
CREATE TABLE #Results (
    ResultId INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(260),
    ColumnName NVARCHAR(128),
    ColumnValue NVARCHAR(MAX),
    ColumnType NVARCHAR(128),
    ValueQuery NVARCHAR(MAX),
    ErrorMessage NVARCHAR(MAX) NULL
)

-- Create helper table for table metadata
CREATE TABLE #TableMetadata (
    TableId INT IDENTITY(1,1) PRIMARY KEY,
    SchemaName NVARCHAR(128),
    TableName NVARCHAR(128),
    QuotedTableName NVARCHAR(260) -- [schema].[table]
)

-- Create helper table for column metadata
CREATE TABLE #ColumnMetadata (
    ColumnId INT IDENTITY(1,1) PRIMARY KEY,
    TableId INT,
    ColumnName NVARCHAR(128),
    QuotedColumnName NVARCHAR(130), -- [column]
    DataType NVARCHAR(128),
    IsSearchable BIT,
    SearchExpression NVARCHAR(500), -- Expression used in WHERE clause for searching
    SelectExpression NVARCHAR(500) -- Expression used to SELECT the value preview
)

-- Create helper table for primary key information
CREATE TABLE #PrimaryKeyInfo (
    PKInfoId INT IDENTITY(1,1) PRIMARY KEY,
    TableId INT,
    PKColumnName NVARCHAR(128),
    QuotedPKColumnName NVARCHAR(130), -- [pk_column]
    PKColumnDataType NVARCHAR(128),
    OrdinalPosition INT -- For composite keys
)

-- Create intermediate table to hold raw search results before query generation
CREATE TABLE #IntermediateResults (
    IntermediateId INT IDENTITY(1,1) PRIMARY KEY,
    TableId INT,
    ColumnId INT,
    FoundValue NVARCHAR(MAX),
    PKValuesXML XML NULL -- Store PK values as XML for easier parsing <row><pk1>val1</pk1><pk2>val2</pk2></row>
)

IF @DebugMode = 1
BEGIN
    SET @EndTime_Init = SYSDATETIME() -- Пример для Init
    PRINT '-- Initialization completed in ' + CAST(DATEDIFF(MILLISECOND, @StartTime_Init, @EndTime_Init) AS VARCHAR(20)) + ' ms --'
END

-- ==========================================================================
-- Metadata Gathering
-- ==========================================================================

IF @DebugMode = 1 
    SET @StartTime_Meta = SYSDATETIME() 

BEGIN TRY
    IF @DebugMode = 1 PRINT '-- Gathering Table Metadata... --'

    -- Gather user tables matching the filter
    INSERT INTO #TableMetadata (SchemaName, TableName, QuotedTableName)
    SELECT
        s.name AS SchemaName,
        t.name AS TableName,
        QUOTENAME(s.name) + '.' + QUOTENAME(t.name) AS QuotedTableName
    FROM sys.tables t -- System tables view
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id -- System schema names
    WHERE t.is_ms_shipped = 0 -- Microsoft system tables
      AND t.name LIKE COALESCE(@TableNameFilter, t.name)

    IF @DebugMode = 1 PRINT 'Found ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' tables.'
    IF @DebugMode = 1 PRINT 'Gathering column and primary key metadata...'

    -- Iterate through each identified table to get column and PK info
    DECLARE @CurrentTableId INT
    DECLARE @CurrentQuotedTableName NVARCHAR(260)
    DECLARE TableCursor CURSOR LOCAL FAST_FORWARD FOR -- Use LOCAL FAST_FORWARD for performance
        SELECT TableId, QuotedTableName FROM #TableMetadata
    
    -- Fetch result set
    OPEN TableCursor
    FETCH NEXT FROM TableCursor INTO @CurrentTableId, @CurrentQuotedTableName

    -- Loop as long as there are more tables to process
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @DebugMode = 1 PRINT 'Processing table: ' + @CurrentQuotedTableName

        -- Gather columns for the current table and insert into #ColumnMetadata
        INSERT INTO #ColumnMetadata (TableId, ColumnName, QuotedColumnName, DataType, IsSearchable, SearchExpression, SelectExpression)
        SELECT
            @CurrentTableId,
            c.name AS ColumnName,
            QUOTENAME(c.name) AS QuotedColumnName,
            UPPER(ty.name) AS DataType,
            -- Determine if the column should be searched based on its type and configuration flags
            CASE
                WHEN ty.name IN ('char', 'varchar', 'nchar', 'nvarchar', 'text', 'ntext', 'uniqueidentifier') THEN 1
                WHEN ty.name IN ('xml') AND @IncludeXml = 1 THEN 1
                WHEN ty.name IN ('datetime', 'smalldatetime', 'date', 'time', 'datetime2', 'datetimeoffset') AND @IncludeDateTime = 1 THEN 1
                WHEN ty.name IN ('tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney') AND @IncludeNumeric = 1 THEN 1
                WHEN ty.name IN ('timestamp', 'rowversion') THEN 1
                ELSE 0
            END AS IsSearchable,
            -- Define the SQL expression used to convert the column value to NVARCHAR for the WHERE clause 
            CASE
                WHEN ty.name IN ('xml') THEN 'CAST(' + QUOTENAME(c.name) + ' AS nvarchar(MAX))'
                WHEN ty.name IN ('timestamp', 'rowversion') THEN 'master.dbo.fn_varbintohexstr('+ QUOTENAME(c.name) + ')'
                WHEN ty.name IN ('datetime', 'smalldatetime', 'date', 'datetime2', 'datetimeoffset') THEN 'CONVERT(nvarchar(50), ' + QUOTENAME(c.name) + ', 121)'
                WHEN ty.name IN ('time') THEN 'CONVERT(nvarchar(50), ' + QUOTENAME(c.name) + ', 114)'
                ELSE 'CAST(' + QUOTENAME(c.name) + ' AS nvarchar(MAX))'
            END AS SearchExpression,
            -- Define the SQL expression used to SELECT a preview of the column value (limited length for strings/XML)
            CASE
                WHEN ty.name IN ('xml') THEN 'LEFT(CAST(' + QUOTENAME(c.name) + ' AS nvarchar(MAX)), ' + CAST(@MaxStringPreviewLength AS VARCHAR(10)) + ')'
                WHEN ty.name IN ('timestamp', 'rowversion') THEN 'master.dbo.fn_varbintohexstr('+ QUOTENAME(c.name) + ')'
                WHEN ty.name IN ('datetime', 'smalldatetime', 'date', 'datetime2', 'datetimeoffset') THEN 'CONVERT(nvarchar(50), ' + QUOTENAME(c.name) + ', 121)'
                WHEN ty.name IN ('time') THEN 'CONVERT(nvarchar(50), ' + QUOTENAME(c.name) + ', 114)'
                ELSE 'LEFT(CAST(' + QUOTENAME(c.name) + ' AS nvarchar(MAX)), ' + CAST(@MaxStringPreviewLength AS VARCHAR(10)) + ')'
            END AS SelectExpression
        FROM sys.columns c
        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        WHERE c.object_id = OBJECT_ID(@CurrentQuotedTableName)
          AND c.name LIKE COALESCE(@ColumnNameFilter, c.name)

        -- Gather primary key info for the current table and insert PK column details into #PrimaryKeyInfo
        INSERT INTO #PrimaryKeyInfo (TableId, PKColumnName, QuotedPKColumnName, PKColumnDataType, OrdinalPosition)
        SELECT
            @CurrentTableId,
            kcu.COLUMN_NAME,
            QUOTENAME(kcu.COLUMN_NAME),
            UPPER(col.DATA_TYPE),
            kcu.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
            AND kcu.TABLE_NAME = tc.TABLE_NAME
        JOIN INFORMATION_SCHEMA.COLUMNS col
            ON kcu.TABLE_SCHEMA = col.TABLE_SCHEMA
            AND kcu.TABLE_NAME = col.TABLE_NAME
            AND kcu.COLUMN_NAME = col.COLUMN_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND kcu.TABLE_SCHEMA = PARSENAME(@CurrentQuotedTableName, 2)
          AND kcu.TABLE_NAME = PARSENAME(@CurrentQuotedTableName, 1)
        ORDER BY kcu.ORDINAL_POSITION

        -- Fetch the next table's data 
        FETCH NEXT FROM TableCursor INTO @CurrentTableId, @CurrentQuotedTableName
    END

    CLOSE TableCursor
    DEALLOCATE TableCursor

    IF @DebugMode = 1
    BEGIN
        SET @EndTime_Meta = SYSDATETIME()
        PRINT '-- Metadata Gathering completed in ' + CAST(DATEDIFF(MILLISECOND, @StartTime_Meta, @EndTime_Meta) AS VARCHAR(20)) + ' ms --'
    END

END TRY -- Handle errors
BEGIN CATCH
    PRINT 'ERROR: Failed during Metadata Gathering.'
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(20))
    PRINT 'Error Message: ' + ERROR_MESSAGE()
    SET NOCOUNT OFF
    RETURN 
END CATCH

-- ==========================================================================
-- Search Execution
-- ==========================================================================

-- Iterate through each searchable column identified earlier 
-- Construct and execute dynamic SQL query to find rows where the column's value matches the @SearchString
BEGIN TRY

    IF @DebugMode = 1 
        SET @StartTime_Search = SYSDATETIME()
        PRINT '-- Starting Search Execution... --'

    -- Variables for dynamic SQL 
    DECLARE @Sql NVARCHAR(MAX)
    DECLARE @QuotedSearchString NVARCHAR(510) = QUOTENAME(@SearchString, '''') -- Quote the search string for dynamic SQL
    DECLARE @CurrentColumnId INT
    DECLARE @CurrentSchemaName NVARCHAR(128)
    DECLARE @CurrentTableName NVARCHAR(128)
    DECLARE @CurrentColumnName NVARCHAR(128)
    DECLARE @CurrentDataType NVARCHAR(128)
    DECLARE @CurrentSearchExpression NVARCHAR(500)
    DECLARE @CurrentSelectExpression NVARCHAR(500)
    DECLARE @PKColumnsForSelect NVARCHAR(MAX)
    DECLARE @PKColumnsForXML NVARCHAR(MAX)
    DECLARE @RowsAffected INT

    -- Iterate through searchable columns marked as IsSearchable=1 from #ColumnMetadata
    DECLARE SearchCursor CURSOR LOCAL FAST_FORWARD FOR -- Use LOCAL FAST_FORWARD for performance
        SELECT
            c.ColumnId, t.SchemaName, t.TableName, t.QuotedTableName, c.ColumnName, c.DataType, c.SearchExpression, c.SelectExpression, c.TableId
        FROM #ColumnMetadata c
        JOIN #TableMetadata t ON c.TableId = t.TableId
        WHERE c.IsSearchable = 1

    -- Fetch the first searchable column's data
    OPEN SearchCursor
    FETCH NEXT FROM SearchCursor INTO @CurrentColumnId, @CurrentSchemaName, @CurrentTableName, @CurrentQuotedTableName, @CurrentColumnName, @CurrentDataType, @CurrentSearchExpression, @CurrentSelectExpression, @CurrentTableId

    -- Loop through each searchable column
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @DebugMode = 1 
            PRINT 'Searching Table: ' + @CurrentQuotedTableName + ', Column: ' + QUOTENAME(@CurrentColumnName)

        SET @PKColumnsForSelect = NULL
        SET @PKColumnsForXML = NULL
        
        -- SELECT statement fragment that generates XML
        SELECT
            @PKColumnsForSelect = COALESCE(@PKColumnsForSelect + ', ', '') + QuotedPKColumnName,
            -- Select the PK values ​​and assign them aliases equal to the names of the PK columns
            @PKColumnsForXML = COALESCE(@PKColumnsForXML + ', ', '') + 'CAST(' + QuotedPKColumnName + ' AS NVARCHAR(MAX)) AS ' + QUOTENAME(PKColumnName)
        FROM #PrimaryKeyInfo
        WHERE TableId = @CurrentTableId
        ORDER BY OrdinalPosition

        -- If no PKs were found, set NULL, If PKs exist, wrap the generated SELECT list into a FOR subquery
        IF @PKColumnsForSelect IS NULL SET @PKColumnsForSelect = 'NULL'
        IF @PKColumnsForXML IS NULL
            SET @PKColumnsForXML = 'NULL'
        ELSE
            -- Use a simpler structure FOR XML PATH('row')
            SET @PKColumnsForXML = '(SELECT ' + @PKColumnsForXML + ' FOR XML PATH(''row''), TYPE)'

        -- Construct the main dynamic SQL to find matches and PKs targeting the #IntermediateResults
        SET @Sql = N'
            INSERT INTO #IntermediateResults (TableId, ColumnId, FoundValue, PKValuesXML)
            SELECT
                ' + CAST(@CurrentTableId AS NVARCHAR(10)) + ',
                ' + CAST(@CurrentColumnId AS NVARCHAR(10)) + ',
                ' + @CurrentSelectExpression + ',
                ' + @PKColumnsForXML + '
            FROM ' + @CurrentQuotedTableName + ' WITH (NOLOCK)
            WHERE ' + @CurrentSearchExpression + ' LIKE ' + @QuotedSearchString + ';
            
            SET @RowCountOUT = @@ROWCOUNT;'; -- Capture the number of rows inserted

        IF @DebugMode = 1 PRINT 'Executing Search SQL:'
        IF @DebugMode = 1 PRINT @Sql

        -- Execute the dynamic SQL and handle errors
        BEGIN TRY
            EXEC sp_executesql @Sql, N'@RowCountOUT INT OUTPUT', @RowsAffected OUTPUT;
            IF @DebugMode = 1 PRINT 'Found ' + ISNULL(CAST(@RowsAffected AS VARCHAR(10)), '0') + ' matches.'
        END TRY
        BEGIN CATCH
            PRINT 'WARNING: Failed to search Table: ' + @CurrentQuotedTableName + ', Column: ' + QUOTENAME(@CurrentColumnName)
            PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(20))
            PRINT 'Error Message: ' + ERROR_MESSAGE()
            PRINT 'SQL Attempted: ' + @Sql
            INSERT INTO #Results (TableName, ColumnName, ColumnType, ErrorMessage)
            VALUES (@CurrentQuotedTableName, QUOTENAME(@CurrentColumnName), @CurrentDataType, 'Search failed: ' + ERROR_MESSAGE())
        END CATCH

        -- Fetch the next searchable column's data
        FETCH NEXT FROM SearchCursor INTO @CurrentColumnId, @CurrentSchemaName, @CurrentTableName, @CurrentQuotedTableName, @CurrentColumnName, @CurrentDataType, @CurrentSearchExpression, @CurrentSelectExpression, @CurrentTableId
    END

    CLOSE SearchCursor
    DEALLOCATE SearchCursor

    IF @DebugMode = 1
    BEGIN
        SET @EndTime_Search = SYSDATETIME() 
        PRINT '-- Search Execution completed in ' + CAST(DATEDIFF(MILLISECOND, @StartTime_Search, @EndTime_Search) AS VARCHAR(20)) + ' ms --'
    END

END TRY -- Handle errors
BEGIN CATCH
    PRINT 'ERROR: Failed during Search Execution setup.'
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(20))
    PRINT 'Error Message: ' + ERROR_MESSAGE()
    SET NOCOUNT OFF
    RETURN 
END CATCH

-- ==========================================================================
-- Query Generation
-- ==========================================================================

-- Iterate through each row found during search #IntermediateResults
-- Generate a query format `SELECT * ... WHERE PK1=value1 AND PK2=value2 ...` using PK values stored in PKValuesXML column
-- Use fallback query if PKs are unavailable

BEGIN TRY

    IF @DebugMode = 1 
        SET @StartTime_QueryGen = SYSDATETIME()
        PRINT '-- Starting Query Generation... --'

    -- Variables for query generation
    DECLARE @ResultId INT
    DECLARE @PKXML XML
    DECLARE @PKWhereClause NVARCHAR(MAX)
    DECLARE @PKColName NVARCHAR(128)
    DECLARE @QuotedPKColNameInLoop NVARCHAR(130)
    DECLARE @PKColDataType NVARCHAR(128)
    DECLARE @PKColValue NVARCHAR(MAX)
    DECLARE @GeneratedQuery NVARCHAR(MAX)
    DECLARE @ErrorMessage NVARCHAR(MAX)

    IF @DebugMode = 1
        BEGIN
            PRINT 'Debug: Sample PKValuesXML from #IntermediateResults'
            SELECT TOP 10 IntermediateId, PKValuesXML
            FROM #IntermediateResults
            WHERE PKValuesXML IS NOT NULL
            PRINT 'End Debug'
        END
        
    -- Iterate through the intermediate results
    DECLARE ResultCursor CURSOR LOCAL FAST_FORWARD FOR -- Use LOCAL FAST_FORWARD for performance
        SELECT ir.IntermediateId, ir.PKValuesXML
        FROM #IntermediateResults ir

    -- Fetch the first result row's data
    OPEN ResultCursor
    FETCH NEXT FROM ResultCursor INTO @ResultId, @PKXML

    -- Loop through each row in #IntermediateResults
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @PKWhereClause = NULL
        SET @GeneratedQuery = NULL
        SET @ErrorMessage = NULL

        -- Generate the query for individual row
        BEGIN TRY
            IF @PKXML IS NOT NULL
            BEGIN
                -- Iterate through the PK columns within the XML
                DECLARE PKCursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT
                        pki.QuotedPKColumnName,
                        pki.PKColumnDataType,
                        pk_vals.c.value('local-name(.)', 'nvarchar(128)') AS NodeName,
                        pk_vals.c.value('.', 'nvarchar(max)') AS NodeValue
                    FROM #IntermediateResults ir
                    JOIN #PrimaryKeyInfo pki ON ir.TableId = pki.TableId
                    CROSS APPLY ir.PKValuesXML.nodes('/row/*') AS pk_vals(c)
                    WHERE ir.IntermediateId = @ResultId AND pki.PKColumnName = pk_vals.c.value('local-name(.)', 'nvarchar(128)')
                    ORDER BY pki.OrdinalPosition

                -- Fetch the first PK column's details from the XML
                OPEN PKCursor
                FETCH NEXT FROM PKCursor INTO @QuotedPKColNameInLoop, @PKColDataType, @PKColName, @PKColValue

                -- Loop through each PK column found in the XML
                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @PKWhereClause = COALESCE(@PKWhereClause + ' AND ', '') + @QuotedPKColNameInLoop + ' = '
                    -- Build WHERE clause fragment for this PK
                    IF @PKColDataType IN ('UNIQUEIDENTIFIER', 'CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'TEXT', 'NTEXT', 'DATE', 'DATETIME', 'SMALLDATETIME', 'DATETIME2', 'DATETIMEOFFSET', 'TIME', 'XML')
                        SET @PKWhereClause = @PKWhereClause + '''' + REPLACE(@PKColValue, '''', '''''') + ''''
                    ELSE IF @PKColDataType IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'REAL', 'MONEY', 'SMALLMONEY', 'BIT')
                        SET @PKWhereClause = @PKWhereClause + ISNULL(@PKColValue, 'NULL')
                    ELSE IF @PKColDataType IN ('TIMESTAMP', 'ROWVERSION', 'BINARY', 'VARBINARY', 'IMAGE')
                         SET @PKWhereClause = @PKWhereClause + '0x' + REPLACE(@PKColValue, '''', '''''')
                    ELSE
                        SET @PKWhereClause = @PKWhereClause + '''' + REPLACE(@PKColValue, '''', '''''') + ''''
                    -- Fetch the next PK column from the XML
                    FETCH NEXT FROM PKCursor INTO @QuotedPKColNameInLoop, @PKColDataType, @PKColName, @PKColValue
                END

                CLOSE PKCursor
                DEALLOCATE PKCursor
                
                -- Finalize the query if the WHERE clause was built
                IF @PKWhereClause IS NOT NULL
                    SET @GeneratedQuery = 'SELECT * FROM ' + (SELECT t.QuotedTableName FROM #IntermediateResults ir JOIN #TableMetadata t ON ir.TableId = t.TableId WHERE ir.IntermediateId = @ResultId) + ' WHERE ' + @PKWhereClause + ';'
                ELSE
                    SET @ErrorMessage = 'Failed to build PK WHERE clause (check PKCursor and XML empty or malformed).'

            END
            ELSE
            -- Generate a fallback query if PK XML is NULL
            BEGIN
                DECLARE @FallbackColumnName NVARCHAR(130)
                DECLARE @FallbackColumnType NVARCHAR(128)
                DECLARE @FallbackColumnValue NVARCHAR(MAX)
                DECLARE @FallbackTableName NVARCHAR(260)

                -- Retrieve details about the original match from #IntermediateResults and related metadata tables
                SELECT
                    @FallbackTableName = t.QuotedTableName,
                    @FallbackColumnName = c.QuotedColumnName,
                    @FallbackColumnType = c.DataType,
                    @FallbackColumnValue = ir.FoundValue
                FROM #IntermediateResults ir
                JOIN #ColumnMetadata c ON ir.ColumnId = c.ColumnId
                JOIN #TableMetadata t ON ir.TableId = t.TableId
                WHERE ir.IntermediateId = @ResultId
                -- If the matched value is likely unique (UUID or short non-wildcard string)
                IF @FallbackColumnType IN ('UNIQUEIDENTIFIER') 
                    OR (@FallbackColumnType IN ('CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR') 
                    AND LEN(@FallbackColumnValue) <= 50 
                    AND CHARINDEX('%', @SearchString) = 0 
                    AND CHARINDEX('_', @SearchString) = 0)
                BEGIN
                    -- Generate a query using the matched value itself in the WHERE clause
                     SET @GeneratedQuery = 'SELECT * FROM ' + @FallbackTableName + 
                        ' WHERE ' + @FallbackColumnName + ' = ''' + REPLACE(@FallbackColumnValue, '''', '''''') + '''; /* Fallback query using matched value */'
                END
                ELSE
                -- Revert to the original search condition
                BEGIN
                    -- Generate a query that re-runs the original LIKE search against the column
                     SET @GeneratedQuery = 'SELECT * FROM ' + @FallbackTableName + 
                        ' WHERE ' + (SELECT c.SearchExpression FROM #ColumnMetadata c WHERE c.ColumnId = (SELECT ColumnId FROM #IntermediateResults WHERE IntermediateId = @ResultId)) + 
                        ' LIKE ' + @QuotedSearchString + '; /* Fallback query using original search. Found value preview: ' + REPLACE(ISNULL(@FallbackColumnValue,'NULL'),'*/','* /') + ' */'
                     SET @ErrorMessage = 'Cannot reliably generate specific row query without PK. Using original search criteria.'
                END
            END

        END TRY -- Handle errors
        BEGIN CATCH
             SET @ErrorMessage = 'ERROR generating query: ' + ERROR_MESSAGE()
             IF @DebugMode = 1 PRINT @ErrorMessage
        END CATCH

        -- Insert the search results and errors into the #Results table for every row processed by the ResultCursor
        INSERT INTO #Results (TableName, ColumnName, ColumnValue, ColumnType, ValueQuery, ErrorMessage)
        SELECT
            t.QuotedTableName,
            c.QuotedColumnName,
            ir.FoundValue,
            c.DataType,
            @GeneratedQuery,
            @ErrorMessage
        FROM #IntermediateResults ir
        JOIN #TableMetadata t ON ir.TableId = t.TableId
        JOIN #ColumnMetadata c ON ir.ColumnId = c.ColumnId
        WHERE ir.IntermediateId = @ResultId

        -- Fetch the next row from the intermediate results
        FETCH NEXT FROM ResultCursor INTO @ResultId, @PKXML
    END

    CLOSE ResultCursor
    DEALLOCATE ResultCursor

    IF @DebugMode = 1
    BEGIN
        SET @EndTime_QueryGen = SYSDATETIME() 
        PRINT '-- Query Generation completed in ' + CAST(DATEDIFF(MILLISECOND, @StartTime_QueryGen, @EndTime_QueryGen) AS VARCHAR(20)) + ' ms --'
    END

END TRY -- Handle errors
BEGIN CATCH
    PRINT 'ERROR: Failed during Query Generation.'
    PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(20))
    PRINT 'Error Message: ' + ERROR_MESSAGE()
    SET NOCOUNT OFF
    RETURN 
END CATCH

-- ==========================================================================
-- Search Results Output
-- ==========================================================================

SET NOCOUNT OFF -- Re-enable for the final SELECT statement

IF @DebugMode = 1 PRINT '-- Displaying search results... --'

-- Search Results Output
SELECT
    TableName, ColumnName, ColumnValue, ColumnType, ValueQuery, ErrorMessage
FROM #Results
ORDER BY TableName, ColumnName

IF @DebugMode = 1
BEGIN
    SET @EndTime_Overall = SYSDATETIME()
    PRINT '-- Script Execution Finished --'
    PRINT '-- Total Script Execution Time: ' + CAST(DATEDIFF(MILLISECOND, @StartTime_Overall, @EndTime_Overall) AS VARCHAR(20)) + ' ms --'
END

-- ==========================================================================
-- Cleanup temporary data
-- ==========================================================================

/*
IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results
IF OBJECT_ID('tempdb..#TableMetadata') IS NOT NULL DROP TABLE #TableMetadata
IF OBJECT_ID('tempdb..#ColumnMetadata') IS NOT NULL DROP TABLE #ColumnMetadata
IF OBJECT_ID('tempdb..#PrimaryKeyInfo') IS NOT NULL DROP TABLE #PrimaryKeyInfo
IF OBJECT_ID('tempdb..#IntermediateResults') IS NOT NULL DROP TABLE #IntermediateResults
*/
