/* DECLARE VARIABLES AND PARAMETERS FOR SEARCH BEHAVIOR */

DECLARE @SearchStrTableName nvarchar(255), 
        @SearchStrColumnName nvarchar(255), 
        @SearchStrColumnValue nvarchar(255), 
        @SearchStrInXML bit, 
        @SearchStrInDateTime bit,
        @FullRowResult bit, 
        @FullRowResultRows int

SET @SearchStrColumnValue = '%YOUR_SEARCH_STRING_HERE%'  /* Uses LIKE syntax */
SET @FullRowResult = 0              /* Output full matching rows, 0 or 1*/
SET @FullRowResultRows = 3          /* Number of full rows to output per one match */
SET @SearchStrTableName = NULL      /* Limit search to a specific table nam, NULL for all tables, uses LIKE syntax */
SET @SearchStrColumnName = NULL     /* Limit search to a specific column name, NULL for all columns, uses LIKE syntax %email% */
SET @SearchStrInXML = 0             /* Include XML columns, 0 or 1. Searching XML data may be slow */
SET @SearchStrInDateTime = 0        /* */

/* CREATE TEMPORARY TABLE FOR RESULTS */

IF OBJECT_ID('tempdb..#Results') IS NOT NULL 
DROP TABLE #Results
CREATE TABLE #Results (
    TableName nvarchar(128), 
    ColumnName nvarchar(128), 
    ColumnValue nvarchar(max),
    ColumnType nvarchar(20))

/* SEARCH ALL TABLES IN THE DATABASE */

SET NOCOUNT ON --turn off row count messages for cleaner output

-- Declare variables for dynamic SQL
DECLARE @TableName nvarchar(256) = '',
        @ColumnName nvarchar(128),
        @ColumnType nvarchar(20), 
        @QuotedSearchStrColumnValue nvarchar(110), 
        @QuotedSearchStrColumnName nvarchar(110)   

-- Quote search string for using in dynamic SQL
SET @QuotedSearchStrColumnValue = QUOTENAME(@SearchStrColumnValue,'''')

-- Temp table to hold column names and data types per table
DECLARE @ColumnNameTable TABLE (
    COLUMN_NAME nvarchar(128),
    DATA_TYPE nvarchar(20))

-- OUTER LOOP: Iterate over all (option) base tables [INFORMATION_SCHEMA.TABLES]
WHILE @TableName IS NOT NULL
BEGIN
    SET @TableName = --next table 
    (
        SELECT MIN(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME))
        FROM    INFORMATION_SCHEMA.TABLES
        WHERE       TABLE_TYPE = 'BASE TABLE'
            AND TABLE_NAME LIKE COALESCE(@SearchStrTableName,TABLE_NAME) --table filter
            AND QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) > @TableName --get the next table
            AND OBJECTPROPERTY(OBJECT_ID(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)), 'IsMSShipped') = 0 --exclude system tables
    )

    IF @TableName IS NOT NULL --process table
    BEGIN

        /* SELECT COLUMNS AND SEARCH FOR MATCHES */

        DECLARE @sql VARCHAR(MAX)

        -- Build dynamic SQL to get all suitable columns by data type [INFORMATION_SCHEMA.COLUMNS]
        SET @sql = 'SELECT QUOTENAME(COLUMN_NAME),DATA_TYPE
            FROM  INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = PARSENAME(''' + @TableName + ''', 2)
            AND TABLE_NAME = PARSENAME(''' + @TableName + ''', 1)
            AND DATA_TYPE IN (' + 
                /* data type filter */
                CASE 
                    WHEN ISNUMERIC(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@SearchStrColumnValue,'%',''),'_',''),'[',''),']',''),'-','')) = 1 
                        THEN '''tinyint'',''int'',''smallint'',''bigint'',''numeric'',''decimal'',''smallmoney'',''money'',' 
                        ELSE '' 
                    END + '''char'',''varchar'',''nchar'',''nvarchar'',''timestamp'',''uniqueidentifier''' + 
                CASE  
                    WHEN @SearchStrInXML = 1 THEN ',''xml''' ELSE '' 
                END + 
                CASE 
                    WHEN @SearchStrInDateTime = 1 
                    THEN ',''datetime'',''smalldatetime'',''date'',''time'',''datetime2'',''datetimeoffset''' ELSE '' 
                END +
                ')
            AND COLUMN_NAME LIKE COALESCE(' + 
                CASE 
                    WHEN @SearchStrColumnName IS NULL 
                    THEN 'NULL' 
                    ELSE '''' + @SearchStrColumnName + '''' 
                END  + 
            ',COLUMN_NAME)'

        -- Insert selected columns into temp table
        INSERT INTO @ColumnNameTable
        EXEC (@sql)

        -- INNER LOOP: Iterate over columns in current table
        WHILE EXISTS (SELECT TOP 1 COLUMN_NAME FROM @ColumnNameTable)
        BEGIN
            PRINT @ColumnName

            -- Get the next column to process
            SELECT TOP 1 @ColumnName = COLUMN_NAME,@ColumnType = DATA_TYPE 
            FROM @ColumnNameTable
            
            -- Build dynamic SQL for searching the value in the column
            SET @sql = 'SELECT ''' + @TableName + ''',''' + @ColumnName + ''',' + 
                CASE @ColumnType 
                    WHEN 'xml' 
                        THEN 'LEFT(CAST(' + @ColumnName + ' AS nvarchar(MAX)), 4096),''' --handle XML columns
                    WHEN 'timestamp' 
                        THEN 'master.dbo.fn_varbintohexstr('+ @ColumnName + '),''' 
                    ELSE 'LEFT(CAST(' + @ColumnName + 'AS nvarchar(MAX)), 4096),''' --handle DateTime columns
                END + @ColumnType + '''
                FROM ' + @TableName + ' (NOLOCK) ' + ' WHERE ' + 
                CASE @ColumnType 
                    WHEN 'xml' 
                        THEN 'CAST(' + @ColumnName + ' AS nvarchar(MAX))' --handle XML columns
                    WHEN 'timestamp' 
                        THEN 'master.dbo.fn_varbintohexstr('+ @ColumnName + ')'
                    ELSE 'CAST(' + @ColumnName + ' AS nvarchar(MAX))' 
                END + ' LIKE ' + @QuotedSearchStrColumnValue --handle DateTime columns 

            -- Run search query and store result
            INSERT INTO #Results
            EXEC(@sql)

            -- Output full matching rows for the first few matches (option)
            IF @@ROWCOUNT > 0 IF @FullRowResult = 1
            BEGIN
                SET @sql = 'SELECT TOP ' + CAST(@FullRowResultRows AS VARCHAR(3)) + 
                    ' ''' + @TableName + ''' AS [TableFound],''' + @ColumnName + ''' AS [ColumnFound],''FullRow>'' AS [FullRow>],*' +
                    ' FROM ' + @TableName + ' (NOLOCK) ' +
                    ' WHERE ' + 
                    CASE @ColumnType 
                        WHEN 'xml' 
                            THEN 'CAST(' + @ColumnName + ' AS nvarchar(MAX))'
                        WHEN 'timestamp' 
                            THEN 'master.dbo.fn_varbintohexstr('+ @ColumnName + ')'
                        ELSE 'CAST(' + @ColumnName + ' AS nvarchar(MAX))'
                    END + ' LIKE ' + @QuotedSearchStrColumnValue
                EXEC(@sql)
            END

            -- Remove processed column from temp list
            DELETE FROM @ColumnNameTable WHERE COLUMN_NAME = @ColumnName
        END
    END
END

/* OUTPUT RESULTS */

SET NOCOUNT OFF --turn row count messages back on

-- Output aggregated results
SELECT TableName, ColumnName, ColumnValue, ColumnType, COUNT(*) AS Count 
FROM #Results
GROUP BY TableName, ColumnName, ColumnValue, ColumnType;
