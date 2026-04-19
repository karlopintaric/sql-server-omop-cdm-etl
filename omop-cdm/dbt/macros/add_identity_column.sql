{% macro add_identity_column(column_name) %}
IF NOT EXISTS (
    SELECT * 
    FROM sys.columns 
    WHERE object_id = OBJECT_ID('{{ this }}') 
    AND name = '{{ column_name }}' 
    AND is_identity = 1
)
BEGIN
    -- Add identity column if not already an identity column
    ALTER TABLE {{ this }} ADD {{ column_name }} INT IDENTITY(1,1);
END
{% endmacro %}
