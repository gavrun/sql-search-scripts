begin;

do $$
declare
    _searchvalue uuid := 'YOUR_SEARCH_STARTS_HERE'; -- 'YOUR_UUID' without % uses exact match for UUIDs
    _searchguid text := trim(both ' ' from _searchvalue::text);
    _srchevttables boolean := true;
    _srchchartypes boolean := true;
    _srchtexttypes boolean := false;
    _chartypecolname text := lower('%id');
    _lowrow integer := 0;
    _strsql text;
    rec record;
begin
    create temp table if not exists tablestobesearched (
        rowid serial,
        tablename text,
        colname text,
        isguidtype boolean,
        sqlquery text
    ) on commit drop;

    create temp table if not exists found (
        rowid integer,
        rowsfound integer
    ) on commit drop;

    truncate tablestobesearched;
    truncate found;

    for rec in
        select c.table_name, c.column_name,
               substring(c.table_name from 1 for 4) = 'evt_' as iseventtable
        from information_schema.columns c
        join information_schema.tables t
          on c.table_name = t.table_name and c.table_schema = t.table_schema
        where c.data_type = 'uuid' and t.table_type = 'BASE TABLE'
    loop
        if not rec.iseventtable or _srchevttables then
            insert into tablestobesearched(tablename, colname, isguidtype, sqlquery)
            values (
                rec.table_name,
                rec.column_name,
                true,
                format('from %s where "%s" = ''%s''',
                       quote_ident(rec.table_name),
                       rec.column_name,
                       _searchguid)
            );
        end if;
    end loop;

    if _srchchartypes then
        for rec in
            select c.table_name, c.column_name, c.character_maximum_length,
                   substring(c.table_name from 1 for 4) = 'evt_' as iseventtable
            from information_schema.columns c
            join information_schema.tables t
              on c.table_name = t.table_name and c.table_schema = t.table_schema
            where c.data_type in ('character varying', 'character', 'varchar', 'char')
              and lower(trim(both ' ' from c.column_name)) like _chartypecolname
              and c.character_maximum_length >= 38
              and t.table_type = 'BASE TABLE'
        loop
            if not rec.iseventtable or _srchevttables then
                insert into tablestobesearched(tablename, colname, isguidtype, sqlquery)
                values (
                    rec.table_name,
                    rec.column_name,
                    false,
                    format('from %s where lower("%s") like ''%%%s%%''',
                           quote_ident(rec.table_name),
                           rec.column_name,
                           _searchguid)
                );
            end if;
        end loop;
    end if;

    if _srchtexttypes then
        for rec in
            select c.table_name, c.column_name,
                   substring(c.table_name from 1 for 4) = 'evt_' as iseventtable
            from information_schema.columns c
            join information_schema.tables t
              on c.table_name = t.table_name and c.table_schema = t.table_schema
            where c.data_type in ('text')
              and t.table_type = 'BASE TABLE'
        loop
            if not rec.iseventtable or _srchevttables then
                insert into tablestobesearched(tablename, colname, isguidtype, sqlquery)
                values (
                    rec.table_name,
                    rec.column_name,
                    null,
                    format('from %s where lower(cast("%s" as text)) like ''%%%s%%''',
                           quote_ident(rec.table_name),
                           rec.column_name,
                           _searchguid)
                );
            end if;
        end loop;
    end if;

    loop
        exit when _lowrow >= (select max(rowid) from tablestobesearched where sqlquery is not null and sqlquery <> '');
        _strsql := (
            select string_agg(
                format('select %s as id, count(*) as cnt %s', rowid::text, sqlquery),
                ' union all '
            )
            from (
                select rowid, sqlquery
                from tablestobesearched
                where rowid > _lowrow
                  and sqlquery is not null
                  and sqlquery <> ''
                order by rowid
                limit 15
            ) as batch
        );
        if _strsql is not null and length(trim(_strsql)) > 0 then
            execute format('insert into found %s', _strsql);
        end if;
        _lowrow := _lowrow + 15;
    end loop;
end $$ language plpgsql;

select
    t.tablename,
    t.colname,
    f.rowsfound,
    'select * ' || t.sqlquery as query
from found f
join tablestobesearched t on f.rowid = t.rowid
where f.rowsfound > 0;


rollback;
