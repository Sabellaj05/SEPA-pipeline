with source as (
    select *
    from {{ iceberg_source('dim_productos') }}
    {% if target.type == 'duckdb' and var('dev_date_filter', false) %}
        where fecha_vigencia >= CURRENT_DATE - INTERVAL '{{ var("dev_days_back", 5) }}' DAY
    {% endif %}
),

renamed as (
    select
        -- ids
        cast(id_producto as string) as id_producto,

        -- properties
        ean,
        descripcion,
        marca,

        -- presentation details
        cantidad_presentacion,
        unidad_medida_presentacion,

        -- snapshot date
        cast(fecha_vigencia as date) as fecha_vigencia
    from source
)

select * from renamed
