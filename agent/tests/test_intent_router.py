import pytest
from unittest.mock import patch

from agent.intent_router import (
    classify_intent,
    extract_search_term,
    format_single_product_shopping_list,
)


def test_extract_search_term() -> None:
    assert extract_search_term("cuánto cuesta la leche?") == "leche"
    assert extract_search_term("¿cuánto sale el asado en coto?") == "asado"
    assert extract_search_term("precio de coca cola 2.25") == "coca cola 2.25"
    assert (
        extract_search_term("buscar yerba playadito de 1kg") == "yerba playadito de 1kg"
    )
    assert extract_search_term("a cuánto está el puré de tomate?") == "puré de tomate"


def test_format_single_product_shopping_list_empty() -> None:
    res = format_single_product_shopping_list("leche inexistente", [])
    assert res["project_name"] == "Búsqueda: Leche Inexistente"
    assert res["total_estimate"] == 0.0
    assert len(res["stores"]) == 1
    assert res["stores"][0]["name"] == "SEPA"


def test_format_single_product_shopping_list_with_quotes() -> None:
    products = [
        {
            "id_producto": "7790742123456",
            "descripcion": "LECHE ENTERA SACHET 1L",
            "marca": "LA SERENISIMA",
            "precio_lista": 1500.0,
            "precio_min": 1400.0,
            "precio_max": 1650.0,
            "sucursales_count": 85,
            "store_quotes": [
                {"cadena_nombre": "Coto", "precio_lista": 1400.0},
                {"cadena_nombre": "Carrefour", "precio_lista": 1450.0},
                {"cadena_nombre": "Dia", "precio_lista": 1650.0},
            ],
        }
    ]

    res = format_single_product_shopping_list("leche", products)
    assert res["project_name"] == "Precio: LECHE ENTERA SACHET 1L"
    assert res["total_estimate"] == 1400.0
    assert res["savings"] == 250.0
    assert len(res["stores"]) == 3
    store_names = [s["name"] for s in res["stores"]]
    assert "Coto" in store_names
    assert "Carrefour" in store_names
    assert "Dia" in store_names


@pytest.mark.asyncio
async def test_classify_intent_heuristics() -> None:
    with patch.dict("os.environ", {"TYPESAFE_API_KEY": ""}, clear=False):
        intent, conf = await classify_intent("cuántas filas cargamos ayer en polaris?")
        assert intent == "lakehouse_analytics"

        intent, conf = await classify_intent("quiero cocinar un asado para 6 amigos")
        assert intent == "recipe_or_shopping_list"

        intent, conf = await classify_intent("cuánto cuesta el puré de tomate?")
        assert intent == "single_product_price"

        intent, conf = await classify_intent("hola cómo estás?")
        assert intent == "general_conversation"
