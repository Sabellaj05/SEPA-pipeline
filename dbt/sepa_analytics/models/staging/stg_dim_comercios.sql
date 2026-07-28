with source as (
    select * from {{ iceberg_source('dim_comercios') }}
),

renamed as (
    select
        -- ids
        cast(id_comercio as string) as id_comercio,
        cast(id_bandera as string) as id_bandera,

        -- dimensions
        cuit,
        razon_social,
        bandera_nombre,
        bandera_url,

        -- snapshot date
        cast(fecha_vigencia as date) as fecha_vigencia
    from source
)

select * from renamed
