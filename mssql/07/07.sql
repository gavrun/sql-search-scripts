/* DECLARE VARIABLES AND PARAMETERS FOR SEARCH BEHAVIOR */

DECLARE @SearchStrTableName nvarchar(255),
        @SearchStrColumnName nvarchar(255),
        @SearchStrColumnValue nvarchar(255),
        @SearchStrInXML bit,
        @SearchStrInDateTime bit

SET @SearchStrColumnValue = '%YOUR_SEARCH_STRING_HERE%'  /* '%YOUR_SEARCH_STRING_HERE%' uses LIKE syntax for searching strings
                                                            'YOUR_UUID_HERE%' uses exact match for UUIDs */
SET @SearchStrTableName = NULL              /* Limit search to a specific table name, NULL for all tables, uses LIKE syntax */
SET @SearchStrColumnName = NULL             /* Limit search to a specific column name, NULL for all columns, uses LIKE syntax %email% */
SET @SearchStrInDateTime = 1                /* Include DateTime columns, 0 or 1. */
SET @SearchStrInXML = 0                     /* Include XML columns, 0 or 1. Searching XML data may be slower */

/* CREATE TEMPORARY TABLE FOR RESULTS */

IF OBJECT_ID('tempdb..#Results') IS NOT NULL
DROP TABLE #Results
CREATE TABLE #Results (
    TableName nvarchar(128),
    ColumnName nvarchar(128),
    ColumnValue nvarchar(max),
    ColumnType nvarchar(20),
    PrimaryKeyWhereClause nvarchar(max), -- WHERE clause based on PK
    Query nvarchar(max) -- SELECT query using PK
)

/* CREATE TEMPORARY TABLE FOR PRIMARY KEY INFO */

IF OBJECT_ID('tempdb..#PKInfo') IS NOT NULL
DROP TABLE #PKInfo
CREATE TABLE #PKInfo (
    COLUMN_NAME nvarchar(128),
    DATA_TYPE nvarchar(128),
    ORDINAL_POSITION int
)

/* SEARCH ALL TABLES IN THE DATABASE */

SET NOCOUNT ON -- turn off row count messages for cleaner output

-- Declare variables for dynamic SQL
DECLARE @TableName nvarchar(256) = '',
        @ColumnName nvarchar(128),
        @ColumnType nvarchar(20),
        @QuotedSearchStrColumnValue nvarchar(110)

-- Quote search string for using in dynamic SQL
SET @QuotedSearchStrColumnValue = QUOTENAME(@SearchStrColumnValue,'''')

PRINT 'DEBUG: Quoted Search Value: ' + @QuotedSearchStrColumnValue

-- Temp table to hold column names and data types per table
DECLARE @ColumnNameTable TABLE (
    COLUMN_NAME nvarchar(128),
    DATA_TYPE nvarchar(20))

/* OUTER LOOP: Iterate over all (option) base tables [INFORMATION_SCHEMA.TABLES] */

WHILE @TableName IS NOT NULL
BEGIN
    SET @TableName = -- next table
    (
        SELECT MIN(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME))
        FROM    INFORMATION_SCHEMA.TABLES
        WHERE       TABLE_TYPE = 'BASE TABLE'
            AND TABLE_NAME LIKE COALESCE(@SearchStrTableName,TABLE_NAME) -- table filter
            AND QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) > @TableName -- get the next table
            AND OBJECTPROPERTY(OBJECT_ID(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)), 'IsMSShipped') = 0 -- exclude system tables
    )

    IF @TableName IS NOT NULL -- process table
    BEGIN
        PRINT 'DEBUG: LOOP'
        PRINT 'DEBUG: Processing Table: ' + @TableName + ' '

        /* GET PRIMARY KEY INFORMATION FOR THE CURRENT TABLE */

        DELETE FROM #PKInfo; -- clear PK info for the new table

        INSERT INTO #PKInfo (COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION)
        SELECT KCU.COLUMN_NAME, C.DATA_TYPE, KCU.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU
            ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
            AND TC.TABLE_SCHEMA = KCU.TABLE_SCHEMA
            AND TC.TABLE_NAME = KCU.TABLE_NAME
        JOIN INFORMATION_SCHEMA.COLUMNS AS C
            ON KCU.TABLE_SCHEMA = C.TABLE_SCHEMA
            AND KCU.TABLE_NAME = C.TABLE_NAME
            AND KCU.COLUMN_NAME = C.COLUMN_NAME
        WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND TC.TABLE_SCHEMA = PARSENAME(@TableName, 2) -- schema
            AND TC.TABLE_NAME = PARSENAME(@TableName, 1)  -- table
        ORDER BY KCU.ORDINAL_POSITION;

        -- Check if Primary Key exists
        IF NOT EXISTS (SELECT 1 FROM #PKInfo)
        BEGIN
            PRINT 'WARNING: No Primary Key found for table ' + @TableName + '. Skipping PK-based query generation for this table. '
        END

        /* BUILD DYNAMIC EXPRESSION FOR PRIMARY KEY WHERE CLAUSE */

        DECLARE @PKWhereExpression NVARCHAR(MAX) = N'';

        SELECT @PKWhereExpression = @PKWhereExpression +
            CASE 
                WHEN @PKWhereExpression <> N'' 
                    THEN N' + '' AND '' + ' ELSE N'' 
                END +
            N'''' + REPLACE(QUOTENAME(pk.COLUMN_NAME),'''','''''') + N' = '' + ' +
            -- Handle quoting/casting based on data type for PK column value
            CASE
                WHEN pk.DATA_TYPE 
                    IN ('varchar', 'nvarchar', 'char', 'nchar', 'uniqueidentifier', 'text', 'ntext', 'xml')
                    THEN N'ISNULL(QUOTENAME(CAST(T.' + QUOTENAME(pk.COLUMN_NAME) + N' AS NVARCHAR(MAX)), ''''''''), ''NULL'')' -- quote strings, handle NULLs
                WHEN pk.DATA_TYPE 
                    IN ('datetime', 'smalldatetime', 'date', 'time', 'datetime2', 'datetimeoffset')
                    THEN N'ISNULL('''''''' + REPLACE(CONVERT(NVARCHAR(50), T.' + QUOTENAME(pk.COLUMN_NAME) + N', 121), '''''''', '''''''''''') + '''''''', ''NULL'')' -- quote dates, handle NULLs
                WHEN pk.DATA_TYPE 
                    IN ('timestamp', 'rowversion', 'binary', 'varbinary', 'image')
                    THEN N'ISNULL(master.dbo.fn_varbintohexstr(T.' + QUOTENAME(pk.COLUMN_NAME) + N'), ''NULL'')' -- convert binary to hex string, handle NULLs
                WHEN pk.DATA_TYPE 
                    IN ('bit')
                    THEN N'ISNULL(CAST(T.' + QUOTENAME(pk.COLUMN_NAME) + N' AS CHAR(1)), ''NULL'')' -- cast bit to 0 or 1
                -- Add other types numeric, float, int etc. which don't need quotes
                WHEN pk.DATA_TYPE 
                    IN ('tinyint', 'smallint', 'int', 'bigint', 'numeric', 'decimal', 'smallmoney', 'money', 'float', 'real')
                    THEN N'ISNULL(CAST(T.' + QUOTENAME(pk.COLUMN_NAME) + N' AS NVARCHAR(MAX)), ''NULL'')' -- cast numbers to string, handle NULLs
                -- Default: treat as string and quote
                ELSE N'ISNULL(QUOTENAME(CAST(T.' + QUOTENAME(pk.COLUMN_NAME) + N' AS NVARCHAR(MAX)), ''''''''), ''NULL'')' 
            END
        FROM #PKInfo pk
        ORDER BY pk.ORDINAL_POSITION
        
        -- If no PKs were found, no WHERE clause can be built, set expression to generate NULL
        IF @PKWhereExpression = N''
        BEGIN
            SET @PKWhereExpression = N'NULL'; -- no PKs
        END
        ELSE
        BEGIN
             -- Wrap the expression construction
             SET @PKWhereExpression = N'(' + @PKWhereExpression + N')';
        END

        PRINT 'DEBUG: PK Where Expression Builder '
        PRINT @PKWhereExpression
        PRINT 'DEBUG: END PK Where Expression Builder '

        /* SELECT COLUMNS AND SEARCH FOR MATCHES */

        DECLARE @sql NVARCHAR(MAX)

        -- Build dynamic SQL to get all suitable columns by data type [INFORMATION_SCHEMA.COLUMNS]
        SET @sql = 'SELECT QUOTENAME(COLUMN_NAME),DATA_TYPE
            FROM  INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = PARSENAME(''' + @TableName + ''', 2)
                AND TABLE_NAME = PARSENAME(''' + @TableName + ''', 1)
                AND DATA_TYPE IN (' +
                    /* data type filter */
                    CASE
                        WHEN ISNUMERIC(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@SearchStrColumnValue,'%',''),'_',''),'[',''),']',''),'-','')) = 1
                            THEN '''tinyint'',''int'',''smallint'',''bigint'',''numeric'',''decimal'',''smallmoney'',''money'',''float'',''real'','
                        ELSE ''
                        END + '''char'',''varchar'',''nchar'',''nvarchar'',''timestamp'',''uniqueidentifier'',''text'',''ntext''' +
                    CASE
                        WHEN @SearchStrInXML = 1 THEN ',''xml''' 
                        ELSE ''
                    END +
                    CASE
                        WHEN @SearchStrInDateTime = 1
                        THEN ',''datetime'',''smalldatetime'',''date'',''time'',''datetime2'',''datetimeoffset''' 
                        ELSE ''
                    END +
                    ')
                AND COLUMN_NAME LIKE COALESCE(' +
                    CASE
                        WHEN @SearchStrColumnName IS NULL THEN 'NULL'
                        ELSE '''' + @SearchStrColumnName + ''''
                    END  +
                ',COLUMN_NAME)'

        PRINT 'DEBUG: Column Selection SQL:'
        PRINT @sql

        -- Clear table for current table
        DELETE FROM @ColumnNameTable

        -- Insert selected columns into temp table
        INSERT INTO @ColumnNameTable
        EXEC (@sql)

        PRINT 'DEBUG: Columns selected for ' + @TableName + '. Count: ' + CAST(@@ROWCOUNT AS VARCHAR(10))

        /* INNER LOOP: Iterate over columns in current table */

        WHILE EXISTS (SELECT TOP 1 COLUMN_NAME FROM @ColumnNameTable)
        BEGIN
            PRINT 'DEBUG: INNER'

            -- Get the next column to process
            SELECT TOP 1 @ColumnName = COLUMN_NAME,@ColumnType = DATA_TYPE
            FROM @ColumnNameTable

            PRINT 'DEBUG: Processing: Column=' + @ColumnName + ', Type=' + @ColumnType

            -- Build dynamic SQL for searching the value and constructing the PK WHERE clause
            DECLARE @ColumnValueExpression NVARCHAR(MAX);
            DECLARE @SearchConditionExpression NVARCHAR(MAX);

            -- Expression to get the display value of the found column
            SET @ColumnValueExpression =
                CASE
                    WHEN @ColumnType = 'xml'
                        THEN N'LEFT(CAST(T.' + @ColumnName + N' AS NVARCHAR(MAX)), 4096)'
                    WHEN @ColumnType = 'timestamp'
                        THEN N'master.dbo.fn_varbintohexstr(T.'+ @ColumnName + N')'
                    WHEN @ColumnType 
                        IN ('datetime', 'smalldatetime', 'date', 'time', 'datetime2', 'datetimeoffset')
                        THEN N'LEFT(CONVERT(NVARCHAR(MAX), T.' + @ColumnName + N', 121), 4096)' -- style 121
                    WHEN @ColumnType 
                        IN ('text', 'ntext')
                        THEN N'LEFT(CAST(T.' + @ColumnName + N' AS NVARCHAR(MAX)), 4096)'
                    ELSE N'LEFT(CAST(T.' + @ColumnName + N' AS NVARCHAR(MAX)), 4096)'
                END

            -- Expression for the search condition in the WHERE clause
            SET @SearchConditionExpression =
                CASE
                    WHEN @ColumnType = 'xml'
                        THEN N'CAST(T.' + @ColumnName + N' AS NVARCHAR(MAX))'
                    WHEN @ColumnType = 'timestamp'
                        THEN N'master.dbo.fn_varbintohexstr(T.'+ @ColumnName + N')'
                    WHEN @ColumnType 
                        IN ('datetime', 'smalldatetime', 'date', 'time', 'datetime2', 'datetimeoffset')
                        THEN N'CONVERT(NVARCHAR(MAX), T.' + @ColumnName + N', 121)' -- search using style 121
                    WHEN @ColumnType 
                        IN ('text', 'ntext')
                         THEN N'CAST(T.' + @ColumnName + N' AS NVARCHAR(MAX))'
                    ELSE N'CAST(T.' + @ColumnName + N' AS NVARCHAR(MAX))'
                END + N' LIKE ' + @QuotedSearchStrColumnValue

            -- Build the main INSERT statement
            SET @sql = N'INSERT INTO #Results (TableName, ColumnName, ColumnValue, ColumnType, PrimaryKeyWhereClause)
                SELECT ''' + REPLACE(@TableName, '''', '''''') + N''', ''' + REPLACE(@ColumnName, '''', '''''') + N''', ' +
                @ColumnValueExpression + N', ''' + @ColumnType + N''', ' +
                @PKWhereExpression + -- include dynamically built PK as WHERE clause string
                N' FROM ' + @TableName + N' AS T (NOLOCK) WHERE ' + @SearchConditionExpression

            PRINT 'DEBUG: Dynamic SQL (INSERT) '
            -- RAISERROR length limit, PRINT long SQL
            IF LEN(@sql) <= 4000 PRINT @sql ELSE PRINT SUBSTRING(@sql, 1, 4000) + '...'
            IF LEN(@sql) > 4000 PRINT SUBSTRING(@sql, 4001, 4000)
            PRINT 'DEBUG: END Dynamic SQL (INSERT) '

            BEGIN TRY
                EXEC(@sql)
            END TRY
            BEGIN CATCH
                 PRINT ERROR_MESSAGE();
                 PRINT 'ERROR: Executing Dynamic SQL:';
                 IF LEN(@sql) <= 4000 PRINT @sql ELSE PRINT SUBSTRING(@sql, 1, 4000) + '...'
                 IF LEN(@sql) > 4000 PRINT SUBSTRING(@sql, 4001, 4000);
                 PRINT 'ERROR: END';
            END CATCH

            -- Remove processed column from temp list
            DELETE FROM @ColumnNameTable WHERE COLUMN_NAME = @ColumnName

            PRINT 'DEBUG: INNER END'
        END

        PRINT 'DEBUG: LOOP END'
    END
END

/* UPDATE QUERY IN RESULTS TABLE */

UPDATE #Results
SET Query = CASE
                WHEN PrimaryKeyWhereClause IS NOT NULL 
                    AND PrimaryKeyWhereClause <> '' 
                    THEN 'SELECT * FROM ' + TableName + ' WHERE ' + PrimaryKeyWhereClause + ';'
                ELSE
                    '--Cannot generate PK-based query (No PK found or error). Found in: ' + TableName + '.' + ColumnName
            END

/* OUTPUT RESULTS */

SET NOCOUNT OFF --turn row count messages back on

-- Output aggregated results
SELECT
    TableName, ColumnName, ColumnValue, ColumnType, COUNT(*) AS MatchCount, MIN(Query) AS SampleQuery -- show one query per group
FROM #Results
GROUP BY TableName, ColumnName, ColumnValue, ColumnType
ORDER BY TableName, ColumnName;

-- DEBUG
-- SELECT * FROM #Results ORDER BY TableName, ColumnName;

-- Clean up temp tables
-- IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results
-- IF OBJECT_ID('tempdb..#PKInfo') IS NOT NULL DROP TABLE #PKInfo
