"""
smart_search.py
---------------
Context-aware product search engine using multi-strategy Elasticsearch queries.

Strategies (combined in a bool/should query):
  S1: match_phrase on productName (boost x5) -- exact phrase, highest reward
  S2: cross_fields multi_match   (boost x3) -- tokens can span multiple fields
  S3: fuzzy multi_match          (boost x1) -- typo tolerance (fuzziness:AUTO)

Usage:
    python smart_search.py "motor capacitor surface mount"   # single query
    python smart_search.py                                    # interactive mode
    python smart_search.py --debug "your query"              # show raw ES response
"""

import sys
import time
import urllib3
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Config ────────────────────────────────────────────────────────────────────
ES_HOST      = "https://127.0.0.1:9200"
ES_USER      = "elastic"
ES_PASS      = "dcJwpNq4m6wtDNpdbUhY"
INDEX_NAME   = "products"
DEFAULT_SIZE = 10


def build_query(query_string: str, size: int = DEFAULT_SIZE) -> dict:
    """Multi-strategy bool/should query for context-aware product retrieval."""
    return {
        "size": size,
        "_source": [
            "productName",
            "productDescription",
            "category.name",
            "category.subCategoryName",
            "productCategory.name",
        ],
        "query": {
            "bool": {
                "should": [
                    # S1: Exact phrase on productName (highest boost)
                    {
                        "match_phrase": {
                            "productName": {
                                "query": query_string,
                                "boost": 5
                            }
                        }
                    },
                    # S2: Cross-fields contextual multi_match
                    # Tokens from the query can match across any of the listed fields.
                    # e.g. "motor cap" can match productName:"capacitor" + category:"AC Motor"
                    {
                        "multi_match": {
                            "query":   query_string,
                            "type":    "cross_fields",
                            "fields": [
                                "productName^4",
                                "productName.autocomplete^2",
                                "category.name^2",
                                "productCategory.name^2",
                                "category.subCategoryName",
                                "productDescription"
                            ],
                            "operator":    "or",
                            "tie_breaker": 0.3,
                            "boost": 3
                        }
                    },
                    # S3: Fuzzy fallback -- handles typos like "polycarbonete" -> "polycarbonate"
                    {
                        "multi_match": {
                            "query":  query_string,
                            "fields": [
                                "productName^3",
                                "category.name",
                                "productCategory.name"
                            ],
                            "fuzziness":      "AUTO",
                            "prefix_length":  2,
                            "max_expansions": 50,
                            "boost": 1
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        },
        "highlight": {
            "fields": {
                "productName":        {"number_of_fragments": 0},
                "productDescription": {"number_of_fragments": 1, "fragment_size": 150},
                "category.name":      {"number_of_fragments": 0}
            },
            "pre_tags":  [">>"],
            "post_tags": ["<<"]
        }
    }


def search(es: Elasticsearch, query_string: str, size: int = DEFAULT_SIZE, debug: bool = False):
    """Execute search and return (response, latency_ms)."""
    query = build_query(query_string, size)
    start = time.perf_counter()
    response = es.search(index=INDEX_NAME, body=query)
    latency_ms = (time.perf_counter() - start) * 1000

    if debug:
        import json
        print("\n-- Query sent --")
        print(json.dumps(query, indent=2))
        print("\n-- Raw response --")
        print(json.dumps(response.body, indent=2, default=str))

    return response, latency_ms


def display_results(response, latency_ms: float, query_string: str):
    """Pretty-print search results (ASCII-safe for all Windows terminals)."""
    hits       = response["hits"]["hits"]
    total_val  = response["hits"]["total"]
    total_hits = total_val["value"] if isinstance(total_val, dict) else total_val

    sep = "=" * 70
    print("\n" + sep)
    print(f"  Query  : \"{query_string}\"")
    print(f"  Total  : {total_hits:,} matches  |  Showing: {len(hits)}  |  Latency: {latency_ms:.1f} ms")
    print(sep)

    if not hits:
        print("  No results found.")
        return

    for i, hit in enumerate(hits, 1):
        src        = hit["_source"]
        score      = round(hit["_score"], 3)
        highlights = hit.get("highlight", {})

        name     = highlights.get("productName", [src.get("productName", "-")])[0]
        desc_raw = highlights.get("productDescription",
                                  [src.get("productDescription", "")])[0]
        desc = desc_raw[:150] if desc_raw else ""
        cat  = src.get("category", {}).get("name", "-")
        sub  = src.get("category", {}).get("subCategoryName", "")
        pcat = src.get("productCategory", {}).get("name", "-")

        print(f"\n  [{i:>2}]  Score: {score}  |  ID: {hit['_id']}")
        print(f"        Name    : {name}")
        print(f"        Category: {cat} > {sub}")
        print(f"        SubType : {pcat}")
        if desc:
            print(f"        Desc    : {desc}...")
        print("-" * 70)


def interactive_mode(es: Elasticsearch, debug: bool = False):
    """Interactive REPL for continuous searching."""
    print("\nElasticsearch Product Search  (type 'exit' to quit)\n")
    while True:
        try:
            query = input("Search > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not query:
            continue
        if query.lower() in {"exit", "quit", "q"}:
            print("Bye!")
            break

        # Optional size override: "basmati rice :20"
        size = DEFAULT_SIZE
        if " :" in query:
            parts = query.rsplit(" :", 1)
            if parts[1].isdigit():
                query = parts[0]
                size  = int(parts[1])

        try:
            resp, latency = search(es, query, size=size, debug=debug)
            display_results(resp, latency, query)
        except Exception as ex:
            print(f"  Search error: {ex}")


# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args  = sys.argv[1:]
    debug = "--debug" in args
    args  = [a for a in args if a != "--debug"]

    es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASS), verify_certs=False)
    try:
        es.info()
    except Exception as e:
        print(f"Cannot connect to Elasticsearch: {e}")
        sys.exit(1)

    if args:
        query_string = " ".join(args)
        resp, latency = search(es, query_string, debug=debug)
        display_results(resp, latency, query_string)
    else:
        interactive_mode(es, debug=debug)
