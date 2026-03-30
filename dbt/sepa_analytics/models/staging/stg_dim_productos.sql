with source as (
    select *
    from {{ iceberg_source('dim_productos') }}
    {% if target.type == 'duckdb' and var('dev_date_filter', false) %}
        WHERE fecha_vigencia_day >= CURRENT_DATE - INTERVAL '{{ var("dev_days_back", 5) }}' DAY
    {% endif %}
),

deduplicated as (
    select *
    from source
    {% if target.type == 'duckdb' %}
        qualify row_number() over (partition by id_producto order by fecha_vigencia_day desc) = 1
    {% else %}
        qualify row_number() over (partition by id_producto order by fecha_vigencia desc) = 1
    {% endif %}
),

renamed as (
    select
        -- ids
        cast(id_producto as string) as id_producto,
        cast(id_comercio as string) as id_comercio,
        cast(id_bandera as string) as id_bandera,
        cast(id_sucursal as string) as id_sucursal,

        -- properties
        productos_ean as ean,
        productos_descripcion as descripcion,
        productos_marca as marca,

        -- presentation details
        productos_cantidad_presentacion as cantidad_presentacion,
        productos_unidad_medida_presentacion as unidad_medida_presentacion,

        -- reference details
        productos_precio_referencia as precio_referencia,
        productos_cantidad_referencia as cantidad_referencia,
        productos_unidad_medida_referencia as unidad_medida_referencia,

        -- promo descriptions
        productos_leyenda_promo1 as leyenda_promo1,
        productos_leyenda_promo2 as leyenda_promo2
    from deduplicated
)

select * from renamed
