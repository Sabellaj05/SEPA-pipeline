{% macro categorize_product(description_column) %}
  {#
    Categorizes a product based on keyword matching against its description.
    Returns one of ~15 IPC-relevant categories used for statistical analysis.

    Usage: {{ categorize_product('descripcion_clean') }} AS categoria_inferida
  #}
  CASE
    -- Lacteos
    WHEN UPPER({{ description_column }}) LIKE '%LECHE%' THEN 'Lacteos'
    WHEN UPPER({{ description_column }}) LIKE '%YOGUR%' THEN 'Lacteos'
    WHEN UPPER({{ description_column }}) LIKE '%QUESO%' THEN 'Lacteos'
    WHEN UPPER({{ description_column }}) LIKE '%MANTECA%' THEN 'Lacteos'
    WHEN UPPER({{ description_column }}) LIKE '%CREMA%' AND UPPER({{ description_column }}) LIKE '%LECHE%' THEN 'Lacteos'

    -- Aceites
    WHEN UPPER({{ description_column }}) LIKE '%ACEITE%' THEN 'Aceites'

    -- Infusiones
    WHEN UPPER({{ description_column }}) LIKE '%YERBA%' THEN 'Infusiones'
    WHEN UPPER({{ description_column }}) LIKE '%CAFE %' OR UPPER({{ description_column }}) LIKE '%CAFÉ%' THEN 'Infusiones'
    WHEN UPPER({{ description_column }}) LIKE '%TE %' AND UPPER({{ description_column }}) NOT LIKE '%ACEITE%' THEN 'Infusiones'
    WHEN UPPER({{ description_column }}) LIKE '%MATE COCIDO%' THEN 'Infusiones'

    -- Panificados
    WHEN UPPER({{ description_column }}) LIKE '%PAN %' OR UPPER({{ description_column }}) LIKE '%PAN LACTAL%' THEN 'Panificados'
    WHEN UPPER({{ description_column }}) LIKE '%HARINA%' THEN 'Panificados'
    WHEN UPPER({{ description_column }}) LIKE '%GALLETITA%' OR UPPER({{ description_column }}) LIKE '%GALLETA%' THEN 'Panificados'

    -- Pastas
    WHEN UPPER({{ description_column }}) LIKE '%FIDEOS%' OR UPPER({{ description_column }}) LIKE '%PASTA%' THEN 'Pastas'
    WHEN UPPER({{ description_column }}) LIKE '%RAVIOLES%' OR UPPER({{ description_column }}) LIKE '%ÑOQUI%' THEN 'Pastas'

    -- Arroz y Legumbres
    WHEN UPPER({{ description_column }}) LIKE '%ARROZ%' THEN 'Arroz y Legumbres'
    WHEN UPPER({{ description_column }}) LIKE '%LENTEJAS%' OR UPPER({{ description_column }}) LIKE '%POROTOS%' THEN 'Arroz y Legumbres'

    -- Azucar
    WHEN UPPER({{ description_column }}) LIKE '%AZUCAR%' OR UPPER({{ description_column }}) LIKE '%AZÚCAR%' THEN 'Azucar'

    -- Bebidas Alcoholicas
    WHEN UPPER({{ description_column }}) LIKE '%CERVEZA%' THEN 'Bebidas Alcoholicas'
    WHEN UPPER({{ description_column }}) LIKE '%VINO%' THEN 'Bebidas Alcoholicas'
    WHEN UPPER({{ description_column }}) LIKE '%FERNET%' THEN 'Bebidas Alcoholicas'
    WHEN UPPER({{ description_column }}) LIKE '%WHISKY%' OR UPPER({{ description_column }}) LIKE '%VODKA%' THEN 'Bebidas Alcoholicas'

    -- Gaseosas
    WHEN UPPER({{ description_column }}) LIKE '%COCA%COLA%' THEN 'Gaseosas'
    WHEN UPPER({{ description_column }}) LIKE '%PEPSI%' THEN 'Gaseosas'
    WHEN UPPER({{ description_column }}) LIKE '%SPRITE%' THEN 'Gaseosas'
    WHEN UPPER({{ description_column }}) LIKE '%FANTA%' THEN 'Gaseosas'
    WHEN UPPER({{ description_column }}) LIKE '%7%UP%' OR UPPER({{ description_column }}) LIKE '%7UP%' THEN 'Gaseosas'
    WHEN UPPER({{ description_column }}) LIKE '%GASEOSA%' THEN 'Gaseosas'

    -- Aguas
    WHEN UPPER({{ description_column }}) LIKE '%AGUA MINERAL%' OR UPPER({{ description_column }}) LIKE '%AGUA SIN GAS%' THEN 'Aguas'
    WHEN UPPER({{ description_column }}) LIKE '%AGUA CON GAS%' THEN 'Aguas'

    -- Carnes y Fiambres
    WHEN UPPER({{ description_column }}) LIKE '%JAMON%' OR UPPER({{ description_column }}) LIKE '%JAMÓN%' THEN 'Carnes y Fiambres'
    WHEN UPPER({{ description_column }}) LIKE '%SALCHICH%' THEN 'Carnes y Fiambres'
    WHEN UPPER({{ description_column }}) LIKE '%MORTADELA%' THEN 'Carnes y Fiambres'

    -- Limpieza
    WHEN UPPER({{ description_column }}) LIKE '%DETERGENTE%' THEN 'Limpieza'
    WHEN UPPER({{ description_column }}) LIKE '%LAVANDINA%' THEN 'Limpieza'
    WHEN UPPER({{ description_column }}) LIKE '%JABON%ROPA%' OR UPPER({{ description_column }}) LIKE '%JABÓN%ROPA%' THEN 'Limpieza'
    WHEN UPPER({{ description_column }}) LIKE '%DESODORANTE%AMBIENTE%' THEN 'Limpieza'
    WHEN UPPER({{ description_column }}) LIKE '%LIMPIADOR%' THEN 'Limpieza'

    -- Higiene Personal
    WHEN UPPER({{ description_column }}) LIKE '%SHAMPOO%' OR UPPER({{ description_column }}) LIKE '%SHAMPU%' THEN 'Higiene Personal'
    WHEN UPPER({{ description_column }}) LIKE '%JABON%TOCADOR%' OR UPPER({{ description_column }}) LIKE '%JABÓN%TOCADOR%' THEN 'Higiene Personal'
    WHEN UPPER({{ description_column }}) LIKE '%PASTA DENTAL%' OR UPPER({{ description_column }}) LIKE '%DENTIFRICO%' THEN 'Higiene Personal'
    WHEN UPPER({{ description_column }}) LIKE '%DESODORANTE%' AND UPPER({{ description_column }}) NOT LIKE '%AMBIENTE%' THEN 'Higiene Personal'
    WHEN UPPER({{ description_column }}) LIKE '%TOALLA%' AND UPPER({{ description_column }}) LIKE '%FEMENIN%' THEN 'Higiene Personal'
    WHEN UPPER({{ description_column }}) LIKE '%PAÑAL%' THEN 'Higiene Personal'

    -- Conservas y Enlatados
    WHEN UPPER({{ description_column }}) LIKE '%ATUN%' OR UPPER({{ description_column }}) LIKE '%ATÚN%' THEN 'Conservas'
    WHEN UPPER({{ description_column }}) LIKE '%TOMATE%PELADO%' OR UPPER({{ description_column }}) LIKE '%TOMATE%TRITURADO%' THEN 'Conservas'
    WHEN UPPER({{ description_column }}) LIKE '%SALSA%TOMATE%' THEN 'Conservas'

    -- Snacks
    WHEN UPPER({{ description_column }}) LIKE '%PAPAS FRITAS%' OR UPPER({{ description_column }}) LIKE '%CHIZITO%' THEN 'Snacks'
    WHEN UPPER({{ description_column }}) LIKE '%PALITO%' AND UPPER({{ description_column }}) LIKE '%SALADO%' THEN 'Snacks'

    ELSE 'Otros'
  END
{% endmacro %}


{% macro clean_description(description_column) %}
  {#
    Strips common prefixes from SEPA product descriptions.
    - DISC: / DISC  = "discontinued" or "discount" prefix added by some chains
    - HVDC = another chain-specific prefix
  #}
  REGEXP_REPLACE(
    REGEXP_REPLACE({{ description_column }}, {{ "'DISC[: ]+'" if target.type == 'duckdb' else "r'^DISC[: ]+'" }}  , ''),
    {{ "'HVDC[: ]+'" if target.type == 'duckdb' else "r'^HVDC[: ]+'" }}, ''
  )
{% endmacro %}
