with source as (
    select *
    from {{ iceberg_source('precios') }}
    {% if target.type == 'duckdb' and var('dev_date_filter', false) %}
        where fecha_vigencia >= CURRENT_DATE - INTERVAL '{{ var("dev_days_back", 5) }}' DAY
    {% elif target.type != 'duckdb' %}
        where fecha_vigencia >= '{{ var("start_date") }}'
    {% endif %}
),

renamed as (
    select
        -- ids
        cast(id_comercio as string) as id_comercio,
        cast(id_bandera as string) as id_bandera,
        cast(id_sucursal as string) as id_sucursal,
        cast(id_producto as string) as id_producto,

        -- dimensions cached on fact (optional use)
        ean as is_ean_valid,
        descripcion,
        marca,
        cantidad_presentacion,
        unidad_medida_presentacion,
        precio_referencia,
        cantidad_referencia,
        unidad_medida_referencia,

        -- fact metrics
        cast(precio_lista as {{ dbt.type_float() }}) as precio_lista,

        -- promo details
        cast(precio_promo1 as {{ dbt.type_float() }}) as precio_unitario_promo1,
        leyenda_promo1,
        cast(precio_promo2 as {{ dbt.type_float() }}) as precio_unitario_promo2,
        leyenda_promo2,

        -- ingestion metadata
        cast(scraped_at as {{ dbt.type_timestamp() }}) as scraped_at,
        cast(fecha_vigencia as date) as fecha_vigencia

    from source
)

select * from renamed
