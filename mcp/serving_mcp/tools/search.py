import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor
from typesafe_sdk import Noul, NoulCriteria, TypeSafeClient

from serving_mcp.clients.serving import get_pg_connection

logger = logging.getLogger(__name__)

_typesafe_client = None

RE_RANK_CRITERIA = NoulCriteria(
    true="The supermarket item is an exact or direct match for the requested grocery product.",
    false="The item is an unrelated product, a different grocery category, a pre-cooked meal containing the item as an ingredient, or an incompatible flavored variant.",
)


def _get_typesafe_client() -> TypeSafeClient | None:
    global _typesafe_client
    if _typesafe_client is None:
        api_key = os.getenv("TYPESAFE_API_KEY")
        if api_key:
            try:
                _typesafe_client = TypeSafeClient()
            except Exception as e:
                logger.warning(f"Could not initialize TypeSafe client: {e}")
                _typesafe_client = None
    return _typesafe_client


try:
    from langfuse import observe
except ImportError:

    def observe(*args: Any, **kwargs: Any) -> Any:
        def decorator(fn: Any) -> Any:
            return fn

        return decorator


@observe(name="postgres_pg_trgm_recall")
def _pg_trgm_recall(cur: Any, query: str, limit: int = 25) -> List[Dict[str, Any]]:
    sql = """
        SET pg_trgm.similarity_threshold = 0.18;
        SELECT
            p.id_producto,
            p.descripcion,
            p.marca,
            p.precio_avg as precio_lista,
            p.precio_min,
            p.precio_max,
            p.sucursales_count,
            round(similarity(public.immutable_unaccent(p.descripcion), public.immutable_unaccent(%s))::numeric, 3) as score
        FROM serving.products p
        WHERE public.immutable_unaccent(p.descripcion) %% public.immutable_unaccent(%s)
           OR public.immutable_unaccent(coalesce(p.marca, '')) %% public.immutable_unaccent(%s)
           OR p.descripcion ILIKE %s
        ORDER BY score DESC
        LIMIT %s;
    """
    wildcard = f"%{query}%"
    cur.execute(sql, (query, query, query, wildcard, limit))
    return [dict(r) for r in cur.fetchall()]


@observe(name="typesafe_jev_rerank")
def _jev_semantic_rerank(
    candidates: List[Dict[str, Any]],
    query: str,
    client: TypeSafeClient,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    question = Noul(
        instructions=f"Does the supermarket item match the requested grocery product: '{query}'?",
        criteria=RE_RANK_CRITERIA,
    )

    def score_candidate(cand: Dict[str, Any]) -> tuple[Dict[str, Any], float]:
        desc = cand.get("descripcion", "")
        marca = cand.get("marca", "")
        state = {
            "requested_query": query,
            "product_description": desc,
            "brand": marca,
        }
        try:
            resp = client.system_one(
                state=state,
                questions={"match": question},
                model="jev-latest",
            )
            s = resp.nouls["match"].noul
        except Exception as e:
            logger.warning(f"Jev scoring error for {desc}: {e}")
            s = 0.0
        return cand, s

    with ThreadPoolExecutor(max_workers=8) as executor:
        scored = list(executor.map(score_candidate, candidates))

    for cand, s in scored:
        cand["semantic_score"] = round(s, 4)

    scored.sort(key=lambda x: (x[1], x[0].get("score", 0.0)), reverse=True)
    return [p[0] for p in scored[:limit]]


@observe(name="enrich_store_quotes")
def _enrich_store_quotes(
    cur: Any, winners: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    prod_ids = [w["id_producto"] for w in winners]
    if not prod_ids:
        return winners

    cur.execute(
        """
        SELECT
            id_producto,
            sucursal_nombre,
            cadena_nombre,
            precio_lista,
            precio_promo1
        FROM serving.store_prices
        WHERE id_producto = ANY(%s)
        ORDER BY precio_lista ASC;
    """,
        (prod_ids,),
    )
    quotes_by_prod: dict[Any, list[dict]] = {}
    for q in cur.fetchall():
        p_id = q["id_producto"]
        if p_id not in quotes_by_prod:
            quotes_by_prod[p_id] = []
        if len(quotes_by_prod[p_id]) < 3:
            quotes_by_prod[p_id].append(dict(q))

    for w in winners:
        p_id = w["id_producto"]
        quotes = quotes_by_prod.get(p_id, [])
        if quotes:
            best_q = quotes[0]
            w["sucursal_nombre"] = best_q.get("sucursal_nombre")
            w["cadena_nombre"] = best_q.get("cadena_nombre")
            w["precio_lista"] = float(best_q.get("precio_lista") or w["precio_lista"])
            w["store_quotes"] = quotes
        else:
            w["sucursal_nombre"] = "Supermercado SEPA"
            w["cadena_nombre"] = "Cadena SEPA"
            w["store_quotes"] = []

    return winners


@observe(name="sepa_hybrid_search", as_type="retriever")
def search_products(
    query: str, limit: int = 10, use_jev: bool = True
) -> List[Dict[str, Any]]:
    """
    Search for grocery products using PostgreSQL pg_trgm (Stage 1)
    and TypeSafe Jev semantic re-ranking (Stage 2).
    """
    conn = get_pg_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        candidates = _pg_trgm_recall(cur, query, limit=25)
        if not candidates:
            return []

        client = _get_typesafe_client() if use_jev else None
        if client:
            winners = _jev_semantic_rerank(candidates, query, client, limit=limit)
        else:
            winners = candidates[:limit]

        return _enrich_store_quotes(cur, winners)

    except Exception as e:
        logger.error(f"Error running hybrid search: {e}", exc_info=True)
        return [{"error": str(e)}]
