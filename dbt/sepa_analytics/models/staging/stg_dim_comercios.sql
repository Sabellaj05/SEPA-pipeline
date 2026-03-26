with source as (
    select * from {{ source('sepa_silver', 'dim_comercios') }}
),

renamed as (
    select
        -- ids
        cast(id_comercio as string) as id_comercio,
        cast(id_bandera as string) as id_bandera,
        
        -- dimensions
        comercio_cuit as cuit,
        comercio_razon_social as razon_social,
        comercio_bandera_nombre as bandera_nombre,
        comercio_bandera_url as bandera_url,
        
        -- metadata
        comercio_version_sepa as version_sepa,
        comercio_ultima_actualizacion as ultima_actualizacion
    from source
)

select * from renamed
