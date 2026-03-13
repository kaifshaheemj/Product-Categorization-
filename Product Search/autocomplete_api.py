"""
autocomplete_api.py
-------------------
FastAPI backend for professional e-commerce autocomplete + full search.

Endpoints:
  GET /autocomplete?q=...&size=8   -> instant suggestions as you type
  GET /search?q=...&page=1&size=20 -> full search results page

Run with:
    C:\\DataGenie\\dg_venv\\Scripts\\python.exe -m uvicorn autocomplete_api:app --reload --port 8000
"""

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
import urllib3, time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="Product Search API", version="1.0")

# Allow the HTML file to call this API (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Elasticsearch connection ──────────────────────────────────────────────────
es = Elasticsearch(
    "https://127.0.0.1:9200",
    basic_auth=("elastic", "dcJwpNq4m6wtDNpdbUhY"),
    verify_certs=False,
)
INDEX = "products"


# ── Autocomplete endpoint ─────────────────────────────────────────────────────
@app.get("/autocomplete")
def autocomplete(q: str = Query(..., min_length=1), size: int = Query(default=8, le=20)):
    """
    Returns instant suggestions for a partial query.
    Uses edge n-gram (productName.autocomplete) + phrase prefix for best results.
    """
    if not q.strip():
        return {"suggestions": [], "took_ms": 0}

    start = time.perf_counter()

    query = {
        "size": size,
        "_source": ["productName", "category.name", "productCategory.name"],
        "query": {
            "bool": {
                "should": [
                    # Phrase prefix: exact start match (highest relevance)
                    {
                        "match_phrase_prefix": {
                            "productName": {
                                "query":             q,
                                "max_expansions":    30,
                                "boost":             5
                            }
                        }
                    },
                    # Edge n-gram: handles mid-word and partial tokens
                    {
                        "match": {
                            "productName.autocomplete": {
                                "query":    q,
                                "operator": "and",
                                "boost":    3
                            }
                        }
                    },
                    # Category name autocomplete
                    {
                        "match_phrase_prefix": {
                            "productCategory.name": {
                                "query":          q,
                                "max_expansions": 20,
                                "boost":          1
                            }
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        },
        # Collapse by category name to show diverse results
        "collapse": {
            "field": "productName.keyword"
        },
        "highlight": {
            "fields": {
                "productName": {
                    "number_of_fragments": 0,
                    "pre_tags":  ["<mark>"],
                    "post_tags": ["</mark>"]
                }
            }
        }
    }

    try:
        resp = es.search(index=INDEX, body=query)
    except Exception as e:
        return {"suggestions": [], "error": str(e), "took_ms": 0}

    took_ms = round((time.perf_counter() - start) * 1000, 1)

    suggestions = []
    for hit in resp["hits"]["hits"]:
        src  = hit["_source"]
        highlights = hit.get("highlight", {})
        name_hl = highlights.get("productName", [src.get("productName", "")])[0]

        suggestions.append({
            "id":           hit["_id"],
            "productName":  src.get("productName", ""),
            "nameHighlight": name_hl,
            "category":     src.get("category", {}).get("name", ""),
            "subType":      src.get("productCategory", {}).get("name", ""),
            "score":        round(hit["_score"], 2)
        })

    return {
        "suggestions": suggestions,
        "count":       len(suggestions),
        "took_ms":     took_ms
    }


# ── Full search endpoint ───────────────────────────────────────────────────────
@app.get("/search")
def search(
    q:    str = Query(..., min_length=1),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, le=50),
    category: str = Query(default=None)
):
    """Full search with pagination and optional category filter."""
    if not q.strip():
        return {"results": [], "total": 0, "page": page, "took_ms": 0}

    start  = time.perf_counter()
    offset = (page - 1) * size

    must_filters = []
    if category:
        must_filters.append({"term": {"category.name.keyword": category}})

    query = {
        "from": offset,
        "size": size,
        "_source": [
            "productName", "productDescription",
            "category.name", "category.subCategoryName",
            "productCategory.name"
        ],
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"match_phrase":    {"productName": {"query": q, "boost": 5}}},
                                {
                                    "multi_match": {
                                        "query":    q,
                                        "type":     "cross_fields",
                                        "fields":   ["productName^4", "category.name^2",
                                                     "productCategory.name^2", "productDescription"],
                                        "operator": "or",
                                        "boost":    3
                                    }
                                },
                                {
                                    "multi_match": {
                                        "query":         q,
                                        "fields":        ["productName^3", "category.name"],
                                        "fuzziness":     "AUTO",
                                        "prefix_length": 2,
                                        "boost":         1
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ],
                "filter": must_filters
            }
        },
        "highlight": {
            "fields": {
                "productName":        {"number_of_fragments": 0, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                "productDescription": {"number_of_fragments": 1, "fragment_size": 200, "pre_tags": ["<mark>"], "post_tags": ["</mark>"]}
            }
        },
        "aggs": {
            "categories": {
                "terms": {"field": "category.name.keyword", "size": 15}
            }
        }
    }

    try:
        resp = es.search(index=INDEX, body=query)
    except Exception as e:
        return {"results": [], "total": 0, "error": str(e), "took_ms": 0}

    took_ms    = round((time.perf_counter() - start) * 1000, 1)
    total_hits = resp["hits"]["total"]["value"]

    results = []
    for hit in resp["hits"]["hits"]:
        src  = hit["_source"]
        hl   = hit.get("highlight", {})
        results.append({
            "id":           hit["_id"],
            "productName":  src.get("productName", ""),
            "nameHighlight": hl.get("productName", [src.get("productName", "")])[0],
            "description":  src.get("productDescription", "")[:200],
            "descHighlight": hl.get("productDescription", [""])[0],
            "category":     src.get("category", {}).get("name", ""),
            "subCategory":  src.get("category", {}).get("subCategoryName", ""),
            "subType":      src.get("productCategory", {}).get("name", ""),
            "score":        round(hit["_score"], 2)
        })

    # Category facets for sidebar filter
    cats = [
        {"name": b["key"], "count": b["doc_count"]}
        for b in resp.get("aggregations", {}).get("categories", {}).get("buckets", [])
    ]

    return {
        "results":    results,
        "total":      total_hits,
        "page":       page,
        "pages":      (total_hits + size - 1) // size,
        "categories": cats,
        "took_ms":    took_ms
    }


@app.get("/")
def root():
    return FileResponse("autocomplete.html")
