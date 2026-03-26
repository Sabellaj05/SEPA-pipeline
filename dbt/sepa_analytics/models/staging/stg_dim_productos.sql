with source as (
    select * from {{ source('sepa_silver', 'dim_productos') }}
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
    from source
)

select * from renamed
