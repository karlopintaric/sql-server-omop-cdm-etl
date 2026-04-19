{% macro drop_all_indexes_on_table(table_name) -%}
    {{ log("Dropping all indexes and constraints on table: " ~ table_name, info=True) }}

    {%- set relation = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier=table_name
    ) %}

    {% if relation is none %}
        {{ log("No relation found for " ~ table_name ~ "; skipping index drop", info=True) }}
        {{ return("") }}
    {% endif %}

    {% set full_table = relation.render() %}
    {% set schema_name = relation.schema %}
    {% set table_only = relation.identifier %}

    {%- set sql %}
    -- Drop XML indexes
    DECLARE @drop_xml_indexes NVARCHAR(MAX);
    SELECT @drop_xml_indexes = (
        SELECT 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL DROP INDEX [' + sys.indexes.[name] + '] ON [' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + OBJECT_NAME(sys.tables.[object_id]) + ']; '
        FROM sys.indexes {{ information_schema_hints() }}
        INNER JOIN sys.tables {{ information_schema_hints() }}
            ON sys.indexes.object_id = sys.tables.object_id
        WHERE sys.indexes.[name] IS NOT NULL
          AND sys.indexes.type_desc = 'XML'
          AND sys.tables.[name] = '{{ table_only }}'
        FOR XML PATH('')
    ); EXEC sp_executesql @drop_xml_indexes;

    -- Drop spatial indexes
    DECLARE @drop_spatial_indexes NVARCHAR(MAX);
    SELECT @drop_spatial_indexes = (
        SELECT 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL DROP INDEX [' + sys.indexes.[name] + '] ON [' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + OBJECT_NAME(sys.tables.[object_id]) + ']; '
        FROM sys.indexes {{ information_schema_hints() }}
        INNER JOIN sys.tables {{ information_schema_hints() }}
            ON sys.indexes.object_id = sys.tables.object_id
        WHERE sys.indexes.[name] IS NOT NULL
          AND sys.indexes.type_desc = 'SPATIAL'
          AND sys.tables.[name] = '{{ table_only }}'
        FOR XML PATH('')
    ); EXEC sp_executesql @drop_spatial_indexes;

    -- Drop foreign key constraints
    DECLARE @drop_fk_constraints NVARCHAR(MAX);
    SELECT @drop_fk_constraints = (
        SELECT 'IF OBJECT_ID(''' + SCHEMA_NAME(CONVERT(VARCHAR(MAX), sys.foreign_keys.[schema_id])) + '.' + sys.foreign_keys.[name] + ''', ''F'') IS NOT NULL ALTER TABLE [' + SCHEMA_NAME(sys.foreign_keys.[schema_id]) + '].[' + OBJECT_NAME(sys.foreign_keys.[parent_object_id]) + '] DROP CONSTRAINT [' + sys.foreign_keys.[name]+ '];'
        FROM sys.foreign_keys
        INNER JOIN sys.tables
            ON sys.foreign_keys.[referenced_object_id] = sys.tables.[object_id]
        WHERE sys.tables.[name] = '{{ table_only }}'
        FOR XML PATH('')
    ); EXEC sp_executesql @drop_fk_constraints;

    -- Drop primary key constraints
    DECLARE @drop_pk_constraints NVARCHAR(MAX);
    SELECT @drop_pk_constraints = (
        SELECT 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL ALTER TABLE [' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + sys.tables.[name] + '] DROP CONSTRAINT [' + sys.indexes.[name]+ '];'
        FROM sys.indexes
        INNER JOIN sys.tables
            ON sys.indexes.[object_id] = sys.tables.[object_id]
        WHERE sys.indexes.is_primary_key = 1
          AND sys.tables.[name] = '{{ table_only }}'
        FOR XML PATH('')
    ); EXEC sp_executesql @drop_pk_constraints;

    -- Drop any remaining indexes
    DECLARE @drop_remaining_indexes NVARCHAR(MAX);
    SELECT @drop_remaining_indexes = (
        SELECT 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL DROP INDEX [' + sys.indexes.[name] + '] ON [' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + OBJECT_NAME(sys.tables.[object_id]) + ']; '
        FROM sys.indexes {{ information_schema_hints() }}
        INNER JOIN sys.tables {{ information_schema_hints() }}
            ON sys.indexes.object_id = sys.tables.object_id
        WHERE sys.indexes.[name] IS NOT NULL
          AND SCHEMA_NAME(sys.tables.schema_id) = '{{ schema_name }}'
          AND sys.tables.[name] = '{{ table_only }}'
        FOR XML PATH('')
    ); EXEC sp_executesql @drop_remaining_indexes;
    {%- endset %}

    {{ log("Executing SQL:\n" ~ sql) }}
    {% do run_query(sql) %}
{%- endmacro %}
