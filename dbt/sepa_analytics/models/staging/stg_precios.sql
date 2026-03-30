with source as (
    select *
    from {{ iceberg_source('precios') }}
    {% if target.type == 'duckdb' and var('dev_date_filter', false) %}
        {# Filter directly on the Hive partition column BEFORE rename.
           This enables DuckDB to prune parquet files at read time. #}
        WHERE fecha_vigencia_day >= CURRENT_DATE - INTERVAL '{{ var("dev_days_back", 5) }}' DAY
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
        productos_ean as is_ean_valid,
        productos_descripcion as descripcion,
        productos_marca as marca,
        productos_cantidad_presentacion as cantidad_presentacion,
        productos_unidad_medida_presentacion as unidad_medida_presentacion,
        productos_precio_referencia as precio_referencia,
        productos_cantidad_referencia as cantidad_referencia,
        productos_unidad_medida_referencia as unidad_medida_referencia,

        -- fact metrics
        cast(productos_precio_lista as {{ dbt.type_float() }}) as precio_lista,

        -- promo details
        cast(productos_precio_unitario_promo1 as {{ dbt.type_float() }}) as precio_unitario_promo1,
        productos_leyenda_promo1 as leyenda_promo1,
        cast(productos_precio_unitario_promo2 as {{ dbt.type_float() }}) as precio_unitario_promo2,
        productos_leyenda_promo2 as leyenda_promo2,

        -- ingestion metadata
        allow_rechunk,
        cast(scraped_at as {{ dbt.type_timestamp() }}) as scraped_at,

        -- partitioning date: DuckDB Hive partition = fecha_vigencia_day, BQ = fecha_vigencia
        {% if target.type == 'duckdb' %}
            cast(fecha_vigencia_day as date) as fecha_vigencia
        {% else %}
            cast(fecha_vigencia as date) as fecha_vigencia
        {% endif %}

    from source
)

select * from renamed
