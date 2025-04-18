SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @searchValue UNIQUEIDENTIFIER;
SET @searchValue = '##PASTE YOUR ID HERE##';

DECLARE 
    @strSql NVARCHAR(MAX),
    @searchguid UNIQUEIDENTIFIER,
    @lowrow SMALLINT,
    @SrchEvtTables BIT,
    @SrchCharTypes BIT,
    @SrchTextTypes BIT,
    @CharTypeColName VARCHAR(200);

SET @searchguid = LTRIM(RTRIM(@searchValue));
SET @SrchEvtTables = 1;
SET @SrchCharTypes = 1;
SET @CharTypeColName = LOWER('%id');
SET @SrchTextTypes = 0;

DECLARE @TablesToBeSearched TABLE
(
    RowId SMALLINT IDENTITY(1,1),
    TableName VARCHAR(255),
    ColName VARCHAR(128),
    IsGuidType BIT,
    SQLQuery NVARCHAR(255)
);

INSERT INTO @TablesToBeSearched
SELECT 
    [TABLE_NAME],
    [COLUMN_NAME],
    [IsGuidType],
    'FROM [' + [TABLE_NAME] + '] WHERE [' + [COLUMN_NAME] + '] = ''' + CAST(@searchguid AS VARCHAR(38)) + ''''
FROM
(
    SELECT 
        isc.[TABLE_NAME],
        isc.[COLUMN_NAME],
        1 AS [IsGuidType],
        CASE 
            WHEN SUBSTRING(isc.[TABLE_NAME], 1, 4) = 'Evt_' THEN 1
            ELSE 0
        END AS [IsEventTable]
    FROM INFORMATION_SCHEMA.COLUMNS isc
    INNER JOIN INFORMATION_SCHEMA.TABLES ist ON isc.[TABLE_NAME] = ist.[TABLE_NAME]
    WHERE isc.[DATA_TYPE] = 'uniqueidentifier'
      AND ist.[TABLE_TYPE] = 'BASE TABLE'
) t1
WHERE [IsEventTable] = 0 OR ([IsEventTable] = 1 AND @SrchEvtTables = 1);

IF (@SrchCharTypes = 1)
BEGIN
    INSERT INTO @TablesToBeSearched
    SELECT 
        [TABLE_NAME],
        [COLUMN_NAME],
        [IsGuidType],
        'FROM [' + [TABLE_NAME] + '] WHERE LOWER([' + [COLUMN_NAME] + ']) LIKE ''%' + LOWER(CAST(@searchguid AS VARCHAR(38))) + '%''' 
    FROM
    (
        SELECT 
            isc.[TABLE_NAME],
            isc.[COLUMN_NAME],
            0 AS [IsGuidType],
            CASE 
                WHEN SUBSTRING(isc.[TABLE_NAME], 1, 4) = 'Evt_' THEN 1
                ELSE 0
            END AS [IsEventTable]
        FROM INFORMATION_SCHEMA.COLUMNS isc
        JOIN INFORMATION_SCHEMA.TABLES ist ON isc.[TABLE_NAME] = ist.[TABLE_NAME]
        WHERE isc.[DATA_TYPE] IN ('char', 'nchar', 'varchar', 'nvarchar')
          AND LOWER(LTRIM(RTRIM(isc.[COLUMN_NAME]))) LIKE LTRIM(RTRIM(@CharTypeColName))
          AND ist.[TABLE_TYPE] = 'BASE TABLE'
          AND isc.[CHARACTER_MAXIMUM_LENGTH] >= 38
    ) t1
    WHERE [IsEventTable] = 0 OR ([IsEventTable] = 1 AND @SrchEvtTables = 1);
END;

IF (@SrchTextTypes = 1)
BEGIN
    INSERT INTO @TablesToBeSearched
    SELECT 
        [TABLE_NAME],
        [COLUMN_NAME],
        [IsGuidType],
        'FROM [' + [TABLE_NAME] + '] WHERE LOWER(CAST([' + [COLUMN_NAME] + '] AS NVARCHAR(MAX))) LIKE ''%' + CAST(@searchguid AS VARCHAR(38)) + '%'''
    FROM
    (
        SELECT 
            isc.[TABLE_NAME],
            isc.[COLUMN_NAME],
            NULL AS [IsGuidType],
            CASE 
                WHEN SUBSTRING(isc.[TABLE_NAME], 1, 4) = 'Evt_' THEN 1
                ELSE 0
            END AS [IsEventTable]
        FROM INFORMATION_SCHEMA.COLUMNS isc
        JOIN INFORMATION_SCHEMA.TABLES ist ON isc.[TABLE_NAME] = ist.[TABLE_NAME]
        WHERE isc.[DATA_TYPE] IN ('text', 'ntext')
          AND ist.[TABLE_TYPE] = 'BASE TABLE'
    ) t1
    WHERE [IsEventTable] = 0 OR ([IsEventTable] = 1 AND @SrchEvtTables = 1);
END;

IF (OBJECT_ID('tempdb..#Found') IS NOT NULL)
    DROP TABLE #Found;

CREATE TABLE #Found (RowId SMALLINT, RowsFound INT);
SET @lowrow = 0;

WHILE (@lowrow < (SELECT MAX(RowId) FROM @TablesToBeSearched WHERE IsGuidType IS NOT NULL))
BEGIN
    SET @strSql = NULL;

    SELECT @strSql = COALESCE(@strSql + ' UNION ALL SELECT ', 'INSERT INTO #Found SELECT ') + 
        '''' + CAST(ttbs.[RowId] AS VARCHAR(10)) + ''' AS [id], COUNT(*) AS [cnt] ' + ttbs.SQLQuery
    FROM @TablesToBeSearched ttbs
    WHERE ttbs.[RowId] BETWEEN @lowrow + 1 AND @lowrow + 15
      AND (IsGuidType = 1 OR @SrchCharTypes = 1);

    EXEC(@strSql);
    SET @lowrow = @lowrow + 15;
END;

SET @lowrow = (SELECT MAX(RowId) FROM @TablesToBeSearched WHERE IsGuidType IS NOT NULL);

IF (@SrchTextTypes = 1)
BEGIN
    WHILE (@lowrow < (SELECT MAX(RowId) FROM @TablesToBeSearched WHERE IsGuidType IS NULL))
    BEGIN
        SET @strSql = NULL;

        SELECT @strSql = COALESCE(@strSql + ' UNION ALL SELECT ', 'INSERT INTO #Found SELECT ') + 
            '''' + CAST(ttbs.[RowId] AS VARCHAR(10)) + ''' AS [id], COUNT(*) AS [cnt] ' + ttbs.SQLQuery
        FROM @TablesToBeSearched ttbs
        WHERE ttbs.[RowId] BETWEEN @lowrow + 1 AND @lowrow + 1
          AND ttbs.[IsGuidType] IS NULL;

        EXEC(@strSql);
        SET @lowrow = @lowrow + 1;
    END;
END;

SELECT DISTINCT 
    ttbs.[TableName],
    ttbs.[ColName],
    f.[RowsFound],
    'SELECT * ' + ttbs.SQLQuery AS QueryText
FROM #Found f
JOIN @TablesToBeSearched ttbs ON f.[RowId] = ttbs.[RowId]
WHERE f.RowsFound > 0;

DROP TABLE #Found;
