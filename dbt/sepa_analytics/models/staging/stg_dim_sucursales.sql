with source as (
    select * from {{ source('sepa_silver', 'dim_sucursales') }}
),

renamed as (
    select
        -- ids
        cast(id_sucursal as string) as id_sucursal,
        cast(id_comercio as string) as id_comercio,
        cast(id_bandera as string) as id_bandera,
        
        -- details
        sucursales_nombre as nombre,
        sucursales_tipo as tipo,
        sucursales_observaciones as observaciones,
        
        -- location
        sucursales_calle as calle,
        sucursales_numero as numero,
        sucursales_barrio as barrio,
        sucursales_codigo_postal as codigo_postal,
        sucursales_localidad as localidad,
        sucursales_provincia as provincia,
        
        -- coordinates
        cast(sucursales_latitud as float64) as latitud,
        cast(sucursales_longitud as float64) as longitud,
        
        -- hours
        sucursales_lunes_horario_atencion as horario_lunes,
        sucursales_martes_horario_atencion as horario_martes,
        sucursales_miercoles_horario_atencion as horario_miercoles,
        sucursales_jueves_horario_atencion as horario_jueves,
        sucursales_viernes_horario_atencion as horario_viernes,
        sucursales_sabado_horario_atencion as horario_sabado,
        sucursales_domingo_horario_atencion as horario_domingo
    from source
)

select * from renamed
