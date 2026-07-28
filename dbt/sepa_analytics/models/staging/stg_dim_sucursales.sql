with source as (
    select * from {{ iceberg_source('dim_sucursales') }}
),

renamed as (
    select
        -- ids
        cast(id_sucursal as string) as id_sucursal,
        cast(id_comercio as string) as id_comercio,
        cast(id_bandera as string) as id_bandera,

        -- details
        nombre,
        tipo,

        -- location
        calle,
        numero,
        barrio,
        codigo_postal,
        localidad,
        provincia,

        -- coordinates
        cast(latitud as {{ dbt.type_float() }}) as latitud,
        cast(longitud as {{ dbt.type_float() }}) as longitud,

        -- snapshot date
        cast(fecha_vigencia as date) as fecha_vigencia
    from source
)

select * from renamed
