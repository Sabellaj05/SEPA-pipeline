with source as (
    select * from {{ iceberg_source('dim_comercios') }}
),

deduplicated as (
    select *
    from source
    {% if target.type == 'duckdb' %}
        qualify row_number() over (partition by id_comercio, id_bandera order by fecha_vigencia_day desc) = 1
    {% else %}
        qualify row_number() over (partition by id_comercio, id_bandera order by fecha_vigencia desc) = 1
    {% endif %}
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
    from deduplicated
)

select * from renamed
