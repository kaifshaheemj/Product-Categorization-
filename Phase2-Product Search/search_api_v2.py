"""
search_api_v2.py
----------------
FastAPI backend for Pepagora product search with business profile cards.

Endpoints:
  GET /autocomplete?q=...&size=8    → instant business suggestions while typing
  GET /search?q=...&page=1&size=20  → full paginated results sorted by tier + relevance

Tire hierarchy (highest first): Global(4) > Scale(3) > Grow(2) > Free(1)
Results within same tier are ordered by Elasticsearch relevance score.

Search strategies (combined in must+should):
  S1: match_phrase on productName        (boost ×6)
  S2: match_phrase on businessName       (boost ×5)
  S3: cross_fields multi_match all text  (boost ×3)
  S4: match on productDescription 70%    (boost ×2, semantic/contextual)
  S5: fuzzy match on name fields         (boost ×1, typo tolerance)

All results are further boosted by tierRank via function_score so Global sellers
always appear before Free-tier sellers at equivalent relevance.

Run:
    python -m uvicorn search_api_v2:app --reload --port 8001
"""

import time
import urllib3
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Pepagora Product Search API v2", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Elasticsearch ─────────────────────────────────────────────────────────────
es = Elasticsearch(
    "https://127.0.0.1:9200",
    basic_auth=("elastic", "dcJwpNq4m6wtDNpdbUhY"),
    verify_certs=False,
)
INDEX = "products_v2"

PEPAGORA_BASE = "https://www.pepagora.com/"

TIER_LABEL = {"global": "Global", "scale": "Scale", "grow": "Grow", "free": "Free"}
TIER_COLOR = {
    "global": "#7c3aed",  # purple
    "scale":  "#1d4ed8",  # blue
    "grow":   "#059669",  # green
    "free":   "#6b7280",  # gray
}


def _build_function_score(inner_query: dict) -> dict:
    """Wrap any query with a function_score that multiplies by tierRank."""
    return {
        "function_score": {
            "query": inner_query,
            "functions": [
                {
                    "field_value_factor": {
                        "field":   "tierRank",
                        "factor":  1.5,        # global (4) gets 6× weight over free (1)
                        "modifier": "log1p",   # dampens extreme differences
                        "missing":  1
                    }
                }
            ],
            "boost_mode":  "multiply",
            "score_mode":  "sum"
        }
    }


def _bool_query(q: str) -> dict:
    """Multi-strategy bool query for product + business search."""
    return {
        "bool": {
            "should": [
                # S1: Exact phrase on productName (highest)
                {"match_phrase": {"productName": {"query": q, "boost": 6}}},
                # S2: Exact phrase on businessName
                {"match_phrase": {"businessName": {"query": q, "boost": 5}}},
                # S3: Cross-fields — tokens can span multiple fields
                {
                    "multi_match": {
                        "query":  q,
                        "type":   "cross_fields",
                        "fields": [
                            "productName^5",
                            "productName.autocomplete^3",
                            "businessName^4",
                            "businessName.autocomplete^2",
                            "categories^2",
                            "businessCountry",
                            "productDescription"
                        ],
                        "operator":    "or",
                        "tie_breaker": 0.3,
                        "boost":       3
                    }
                },
                # S4: Contextual / semantic — description matching with 70% coverage
                {
                    "match": {
                        "productDescription": {
                            "query":                  q,
                            "minimum_should_match":   "70%",
                            "boost":                  2
                        }
                    }
                },
                # S5: Fuzzy — typo tolerance
                {
                    "multi_match": {
                        "query":         q,
                        "fields":        ["productName^3", "businessName^2", "categories"],
                        "fuzziness":     "AUTO",
                        "prefix_length": 2,
                        "boost":         1
                    }
                }
            ],
            "minimum_should_match": 1
        }
    }


def _format_hit(hit: dict) -> dict:
    """Normalise a raw ES hit into the API response shape."""
    src  = hit["_source"]
    hl   = hit.get("highlight", {})
    pkg  = src.get("packageType", "free")
    subdomain = src.get("businessSubDomain", "")
    profile_url = (PEPAGORA_BASE + subdomain) if subdomain else ""

    return {
        "id":              hit["_id"],
        "score":           round(hit["_score"] or 0, 2),
        # Product
        "productName":     src.get("productName", ""),
        "nameHighlight":   (hl.get("productName") or [src.get("productName", "")])[0],
        "description":     src.get("productDescription", "")[:300],
        "descHighlight":   (hl.get("productDescription") or [""])[0],
        # Business profile
        "businessName":    src.get("businessName", ""),
        "businessHighlight": (hl.get("businessName") or [src.get("businessName", "")])[0],
        "businessCountry": src.get("businessCountry", ""),
        "businessLogo":    src.get("businessLogo", ""),
        "profileUrl":      profile_url,
        "categories":      src.get("categories", []),
        # Tier
        "packageType":     pkg,
        "tierLabel":       TIER_LABEL.get(pkg, "Free"),
        "tierColor":       TIER_COLOR.get(pkg, "#6b7280"),
    }


# ── /autocomplete ─────────────────────────────────────────────────────────────
@app.get("/autocomplete")
def autocomplete(
    q:    str = Query(..., min_length=1),
    size: int = Query(default=8, le=20)
):
    """
    Returns deduplicated business suggestions as the user types.
    Results are collapsed by businessName so each business appears once.
    Ordered by tier (Global first) then relevance.
    """
    if not q.strip():
        return {"suggestions": [], "took_ms": 0}

    start = time.perf_counter()

    inner = {
        "bool": {
            "should": [
                {"match_phrase_prefix": {"productName":              {"query": q, "max_expansions": 30, "boost": 5}}},
                {"match_phrase_prefix": {"businessName":             {"query": q, "max_expansions": 30, "boost": 4}}},
                {"match":              {"productName.autocomplete":  {"query": q, "operator": "and", "boost": 3}}},
                {"match":              {"businessName.autocomplete": {"query": q, "operator": "and", "boost": 2}}},
                {"match_phrase_prefix": {"categories":               {"query": q, "max_expansions": 20, "boost": 1}}},
            ],
            "minimum_should_match": 1
        }
    }

    query = {
        "size": size * 3,   # fetch more to compensate collapse dedup
        "_source": [
            "productName", "businessName", "businessCountry",
            "businessLogo", "businessSubDomain", "categories",
            "packageType", "tierRank"
        ],
        "query": _build_function_score(inner),
        # Collapse so each business appears only once
        "collapse": {"field": "businessName.keyword"},
        "highlight": {
            "fields": {
                "productName":  {"number_of_fragments": 0, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                "businessName": {"number_of_fragments": 0, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
            }
        },
        "sort": [
            {"tierRank": {"order": "desc"}},   # Global first
            "_score"
        ]
    }

    try:
        resp = es.search(index=INDEX, body=query)
    except Exception as e:
        return JSONResponse({"suggestions": [], "error": str(e), "took_ms": 0})

    took_ms = round((time.perf_counter() - start) * 1000, 1)

    suggestions = []
    for hit in resp["hits"]["hits"][:size]:
        suggestions.append(_format_hit(hit))

    return {"suggestions": suggestions, "count": len(suggestions), "took_ms": took_ms}


# ── /suggest ──────────────────────────────────────────────────────────────────
@app.get("/suggest")
def suggest(
    q:    str = Query(..., min_length=1),
    size: int = Query(default=6, le=10)
):
    """
    Returns raw product name completions — NOT collapsed by business.
    Used for Section 2 of the autocomplete dropdown (quick search suggestions).
    Each item returns only: productName, nameHighlight, categories.
    """
    if not q.strip():
        return {"items": [], "took_ms": 0}

    start = time.perf_counter()

    query = {
        "size": size,
        "_source": ["productName", "categories"],
        "query": {
            "bool": {
                "should": [
                    {"match_phrase_prefix": {"productName": {"query": q, "max_expansions": 25, "boost": 3}}},
                    {"match": {"productName.autocomplete": {"query": q, "operator": "and", "boost": 2}}},
                    {"match_phrase_prefix": {"categories": {"query": q, "max_expansions": 15, "boost": 1}}},
                ],
                "minimum_should_match": 1
            }
        },
        "highlight": {
            "fields": {
                "productName": {"number_of_fragments": 0, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]}
            }
        },
        # Collapse by productName.keyword so we don't repeat the exact same product title
        "collapse": {"field": "productName.keyword"},
    }

    try:
        resp = es.search(index=INDEX, body=query)
    except Exception as e:
        return JSONResponse({"items": [], "error": str(e), "took_ms": 0})

    took_ms = round((time.perf_counter() - start) * 1000, 1)

    items = []
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        hl  = hit.get("highlight", {})
        items.append({
            "productName":   src.get("productName", ""),
            "nameHighlight": (hl.get("productName") or [src.get("productName", "")])[0],
            "categories":    src.get("categories", []),
        })

    return {"items": items, "took_ms": took_ms}


# ── /search ───────────────────────────────────────────────────────────────────
@app.get("/search")
def search(
    q:        str = Query(..., min_length=1),
    page:     int = Query(default=1, ge=1),
    size:     int = Query(default=20, le=50),
    category: str = Query(default=None),
    country:  str = Query(default=None),
    tier:     str = Query(default=None),
):
    """
    Full paginated product search. Supports optional filters:
      category (exact keyword match on categories.keyword)
      country  (exact keyword match on businessCountry)
      tier     (global/scale/grow/free)

    Results: Global > Scale > Grow > Free, then by relevance.
    """
    if not q.strip():
        return {"results": [], "total": 0, "page": page, "took_ms": 0}

    start  = time.perf_counter()
    offset = (page - 1) * size

    filters = []
    if category:
        filters.append({"term": {"categories.keyword": category}})
    if country:
        filters.append({"term": {"businessCountry": country}})
    if tier:
        filters.append({"term": {"packageType": tier.lower()}})

    inner = {
        "bool": {
            "must":   [_bool_query(q)],
            "filter": filters
        }
    }

    query = {
        "from": offset,
        "size": size,
        "_source": [
            "productName", "productDescription",
            "businessName", "businessCountry", "businessLogo",
            "businessSubDomain", "categories",
            "packageType", "tierRank"
        ],
        "query": _build_function_score(inner),
        "highlight": {
            "fields": {
                "productName":        {"number_of_fragments": 0, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                "businessName":       {"number_of_fragments": 0, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                "productDescription": {"number_of_fragments": 1, "fragment_size": 200, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
            }
        },
        "sort": [
            {"tierRank": {"order": "desc"}},
            "_score"
        ],
        "aggs": {
            "categories": {"terms": {"field": "categories.keyword", "size": 20}},
            "countries":  {"terms": {"field": "businessCountry",     "size": 20}},
            "tiers":      {"terms": {"field": "packageType",          "size": 5}},
        }
    }

    try:
        resp = es.search(index=INDEX, body=query)
    except Exception as e:
        return JSONResponse({"results": [], "total": 0, "error": str(e), "took_ms": 0})

    took_ms    = round((time.perf_counter() - start) * 1000, 1)
    total_hits = resp["hits"]["total"]["value"]

    results = [_format_hit(h) for h in resp["hits"]["hits"]]

    # Aggregation buckets for sidebar filters
    aggs = resp.get("aggregations", {})
    cats = [{"name": b["key"], "count": b["doc_count"]}
            for b in aggs.get("categories", {}).get("buckets", [])]
    ctrs = [{"name": b["key"], "count": b["doc_count"]}
            for b in aggs.get("countries",  {}).get("buckets", [])]
    tier_buckets = [
        {
            "name":  b["key"],
            "label": TIER_LABEL.get(b["key"], b["key"]),
            "color": TIER_COLOR.get(b["key"], "#6b7280"),
            "count": b["doc_count"]
        }
        for b in sorted(
            aggs.get("tiers", {}).get("buckets", []),
            key=lambda x: -{"global": 4, "scale": 3, "grow": 2, "free": 1}.get(x["key"], 0)
        )
    ]

    return {
        "results":    results,
        "total":      total_hits,
        "page":       page,
        "pages":      (total_hits + size - 1) // size,
        "categories": cats,
        "countries":  ctrs,
        "tiers":      tier_buckets,
        "took_ms":    took_ms,
    }


# ── /health ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    try:
        info = es.info()
        count = es.count(index=INDEX)
        return {"status": "ok", "es_version": info["version"]["number"], "docs": count["count"]}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=503)


# ── / (serve frontend) ────────────────────────────────────────────────────────
@app.get("/")
def root():
    return FileResponse("search_ui_v2.html")
