SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @searchValue NVARCHAR(100);
SET @searchValue = N'%YOUR_SEARCH_STRING%'; 

DECLARE 
    @strSql NVARCHAR(MAX),
    @lowrow SMALLINT = 0,
    @SrchEvtTables BIT = 1,
    @isGuid BIT = 0,
    @searchValueTrimmed NVARCHAR(100);

SET @searchValueTrimmed = LTRIM(RTRIM(@searchValue));

IF TRY_CAST(@searchValueTrimmed AS UNIQUEIDENTIFIER) IS NOT NULL AND LEN(@searchValueTrimmed) = 36
    SET @isGuid = 1;

DECLARE @TablesToBeSearched TABLE
(
    RowId SMALLINT IDENTITY(1,1),
    TableName VARCHAR(255),
    ColName VARCHAR(128),
    SQLQuery NVARCHAR(4000)
);

IF (@isGuid = 1)
BEGIN
    INSERT INTO @TablesToBeSearched
    SELECT 
        isc.[TABLE_NAME],
        isc.[COLUMN_NAME],
        'FROM [' + isc.[TABLE_NAME] + '] WHERE [' + isc.[COLUMN_NAME] + '] = ''' + @searchValueTrimmed + ''''
    FROM INFORMATION_SCHEMA.COLUMNS isc
    INNER JOIN INFORMATION_SCHEMA.TABLES ist ON isc.[TABLE_NAME] = ist.[TABLE_NAME]
    WHERE isc.[DATA_TYPE] = 'uniqueidentifier'
      AND ist.[TABLE_TYPE] = 'BASE TABLE';
END;

INSERT INTO @TablesToBeSearched
SELECT 
    isc.[TABLE_NAME],
    isc.[COLUMN_NAME],
    'FROM [' + isc.[TABLE_NAME] + '] WHERE LOWER(CAST([' + isc.[COLUMN_NAME] + '] AS NVARCHAR(MAX))) LIKE ''' + LOWER(@searchValueTrimmed) + ''''
FROM INFORMATION_SCHEMA.COLUMNS isc
JOIN INFORMATION_SCHEMA.TABLES ist ON isc.[TABLE_NAME] = ist.[TABLE_NAME]
WHERE isc.[DATA_TYPE] IN ('char', 'nchar', 'varchar', 'nvarchar')
  AND ist.[TABLE_TYPE] = 'BASE TABLE';

INSERT INTO @TablesToBeSearched
SELECT 
    isc.[TABLE_NAME],
    isc.[COLUMN_NAME],
    'FROM [' + isc.[TABLE_NAME] + '] WHERE LOWER(CAST([' + isc.[COLUMN_NAME] + '] AS NVARCHAR(MAX))) LIKE ''' + LOWER(@searchValueTrimmed) + ''''
FROM INFORMATION_SCHEMA.COLUMNS isc
JOIN INFORMATION_SCHEMA.TABLES ist ON isc.[TABLE_NAME] = ist.[TABLE_NAME]
WHERE isc.[DATA_TYPE] IN ('text', 'ntext')
  AND ist.[TABLE_TYPE] = 'BASE TABLE';

IF OBJECT_ID('tempdb..#Found') IS NOT NULL
    DROP TABLE #Found;

CREATE TABLE #Found (RowId SMALLINT, RowsFound INT);

WHILE (@lowrow < (SELECT MAX(RowId) FROM @TablesToBeSearched))
BEGIN
    SET @strSql = NULL;

    SELECT @strSql = COALESCE(@strSql + ' UNION ALL SELECT ', 'INSERT INTO #Found SELECT ') + 
        '''' + CAST(RowId AS VARCHAR) + ''' AS [id], COUNT(*) AS [cnt] ' + SQLQuery
    FROM @TablesToBeSearched
    WHERE RowId BETWEEN @lowrow + 1 AND @lowrow + 15;

    EXEC(@strSql);
    SET @lowrow += 15;
END;

SELECT 
    t.TableName,
    t.ColName,
    f.RowsFound,
    'SELECT * ' + t.SQLQuery AS [QueryText]
FROM #Found f
JOIN @TablesToBeSearched t ON t.RowId = f.RowId
WHERE f.RowsFound > 0;

DROP TABLE #Found;
