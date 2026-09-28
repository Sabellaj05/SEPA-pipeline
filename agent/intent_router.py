"""Intent router using TypeSafe AI's Jev model (System One).

Classifies incoming user prompts into four operational paths:
- single_product_price: Instant fast-path via PostgreSQL pg_trgm + Jev re-ranking
- recipe_or_shopping_list: Multi-item recipe planner + hybrid search + formatter
- lakehouse_analytics: Lakehouse data pipeline metadata and Iceberg tools
- general_conversation: Chat/greetings
"""

import logging
import os
import re
from typing import Any

from typesafe_sdk import AsyncTypeSafeClient, Choice, ChoiceAnswer

logger = logging.getLogger(__name__)

INTENT_CRITERIA = {
    "single_product_price": (
        "The user wants to check the price, search, or compare a single specific grocery product "
        "(e.g., 'cuánto cuesta la leche', 'precio de coca cola 2.25', 'buscar yerba playadito', "
        "'cuánto sale el kilo de asado', 'a cuánto está el tomate')."
    ),
    "recipe_or_shopping_list": (
        "The user wants to cook a meal, prepare food for people, or build a multi-item grocery shopping list "
        "(e.g., 'quiero hacer empanadas para 6 personas', 'asado para el domingo con amigos', "
        "'ingredientes para un guiso de lentejas', 'armar lista de compras para pizza')."
    ),
    "lakehouse_analytics": (
        "The user asks about data pipeline metrics, loaded tables, row counts, or Iceberg metadata "
        "(e.g., 'cuántas filas cargamos ayer', 'estado de las tablas de SEPA', 'última fecha en polaris')."
    ),
    "general_conversation": (
        "Greetings, bot capabilities, compliments, or off-topic questions "
        "(e.g., 'hola', 'qué podés hacer', 'quién sos', 'gracias')."
    ),
}

CLEAN_PATTERNS = [
    r"^[¿¡\s]*(?:hola\s*,?\s*)?(?:por\s+favor\s*,?\s*)?(?:me\s+dec[ií]s\s+)?(?:cu[aá]nto\s+(?:cuesta|sale|est[aá]|vale|cotiza)[n]?|precio\s+de(?:l|\s+la|\s+los|\s+las)?|a\s+cu[aá]nto\s+est[aá][n]?|busc(?:ar|ame|á)?|ten[eé]s|consultar)\s+(?:el|la|los|las|un|una|de|del)?\s*",
    r"(?:\s+en\s+(?:coto|carrefour|dia|chango\s*m[aá]s|disco|jumbo|supermercados?|\w+)|por\s+favor|[?¿!.,;])+$",
]


def extract_search_term(prompt: str) -> str:
    """Extract canonical product search term from a conversational price question."""
    cleaned = prompt.strip()
    for pattern in CLEAN_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()
    return cleaned or prompt.strip()


def parse_ingredient_lines(text: str) -> list[tuple[str, str]]:
    """Parse recipe planner output into a list of (search_term, quantity_description)."""
    results: list[tuple[str, str]] = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^[-*•\d.]+\s*", "", line).strip()
        if not line:
            continue
        match = re.match(r"^(.*?)\s*\((.*?)\)$", line)
        if match:
            name = match.group(1).strip()
            qty = match.group(2).strip()
        else:
            name = line
            qty = ""
        results.append((name, qty))
    return results


def format_single_product_shopping_list(
    query: str, products: list[dict[str, Any]]
) -> dict[str, Any]:
    """Format single product results into the ShoppingList JSON schema expected by the frontend."""
    if not products:
        return {
            "project_name": f"Búsqueda: {query.title()}",
            "message": f"No encontré precios para '{query}' en las sucursales relevadas de SEPA.",
            "total_estimate": 0.0,
            "savings": 0.0,
            "stores": [
                {
                    "name": "SEPA",
                    "items": [
                        {
                            "name": query,
                            "price": 0.0,
                            "description": "Sin datos",
                            "quantity": 1,
                        }
                    ],
                }
            ],
        }

    top = products[0]
    desc = top.get("descripcion", query)
    marca = top.get("marca") or "Sin marca"
    avg_p = float(top.get("precio_lista") or 0.0)
    min_p = float(top.get("precio_min") or avg_p)
    max_p = float(top.get("precio_max") or avg_p)
    store_quotes = top.get("store_quotes") or top.get("store_prices") or []

    # Group store prices by retail chain
    stores = []
    if store_quotes:
        chains: dict[str, float] = {}
        for sp in store_quotes:
            cadena = sp.get("cadena_nombre") or "Supermercado"
            p = float(sp.get("precio_lista") or sp.get("precio") or avg_p)
            if cadena not in chains or p < chains[cadena]:
                chains[cadena] = p

        for cadena, p in chains.items():
            stores.append(
                {
                    "name": cadena,
                    "items": [
                        {
                            "name": desc,
                            "price": p,
                            "description": f"Marca: {marca}",
                            "quantity": 1,
                        }
                    ],
                }
            )

    if not stores:
        stores.append(
            {
                "name": "Promedio SEPA",
                "items": [
                    {
                        "name": desc,
                        "price": avg_p,
                        "description": f"Marca: {marca}",
                        "quantity": 1,
                    }
                ],
            }
        )

    savings = round(max_p - min_p, 2) if max_p > min_p else 0.0
    msg = (
        f"Encontré **{desc}** ({marca}). El precio promedio es **${avg_p:,.2f}** "
        f"(rango entre ${min_p:,.2f} y ${max_p:,.2f} en {top.get('sucursales_count', 1)} sucursales)."
    )

    return {
        "project_name": f"Precio: {desc}",
        "message": msg,
        "total_estimate": min_p,
        "savings": savings,
        "stores": stores,
    }


try:
    from langfuse import observe
except ImportError:

    def observe(*args: Any, **kwargs: Any) -> Any:
        def decorator(fn: Any) -> Any:
            return fn

        return decorator


@observe(name="intent_router", as_type="agent")
async def classify_intent(prompt: str) -> tuple[str, float]:
    """Classify user intent using TypeSafe Jev Choice with rule-based fallback."""
    api_key = os.getenv("TYPESAFE_API_KEY")
    if api_key:
        try:
            async with AsyncTypeSafeClient() as client:
                resp = await client.system_one(
                    state=prompt,
                    questions={
                        "intent": Choice(
                            instructions="Classify the user's primary intent.",
                            criteria=INTENT_CRITERIA,
                        )
                    },
                    model="jev-latest",
                )
                ans = resp.answers["intent"]
                if isinstance(ans, ChoiceAnswer):
                    return ans.choice, float(ans.confidence or 0.0)
        except Exception as exc:
            logger.warning(
                f"TypeSafe intent classification failed, falling back to heuristics: {exc}"
            )
    # -> Needs more work, although is hard to get to the fallback
    # Heuristic fallback
    p = prompt.lower().strip()
    if any(
        k in p
        for k in [
            "fila",
            "carg",
            "iceberg",
            "polaris",
            "pipeline",
            "lakehouse",
            "tabla",
        ]
    ):
        return "lakehouse_analytics", 0.8
    if any(
        k in p
        for k in [
            "receta",
            "cocinar",
            "personas",
            "amigos",
            "asado",
            "pizza",
            "empanada",
            "guiso",
            "lista",
            "comida",
        ]
    ):
        return "recipe_or_shopping_list", 0.8
    if any(
        p.startswith(k)
        for k in [
            "cuanto",
            "cuánto",
            "precio",
            "a cuanto",
            "a cuánto",
            "buscar",
            "tenes",
            "tenés",
        ]
    ):
        return "single_product_price", 0.8
    return "general_conversation", 0.5
