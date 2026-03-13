"""
products_index_v2.py
--------------------
Creates the 'products_v2' Elasticsearch index with:
  - Business profile fields embedded (businessName, businessLogo, tier, sub-domain)
  - packageType tier ranking: global=4, scale=3, grow=2, free=1 (numeric for fast sort)
  - Custom analyzers: synonym expansion, stemming, edge n-gram autocomplete
  - Fields optimised for sub-10ms p95 latency on 100k docs
  - Single shard, zero replicas (local/single-node deployment)

Usage:
    python products_index_v2.py            # create / recreate
    python products_index_v2.py --show     # print mapping JSON only
    python products_index_v2.py --restore  # restore settings after bulk load
"""

import sys
import json
import urllib3
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Connection ────────────────────────────────────────────────────────────────
ES_HOST    = "https://127.0.0.1:9200"
ES_USER    = "elastic"
ES_PASS    = "dcJwpNq4m6wtDNpdbUhY"
INDEX_NAME = "products_v2"

es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASS), verify_certs=False)

# ── Tier rank mapping (for function_score boosting) ───────────────────────────
TIER_RANK = {"global": 4, "scale": 3, "grow": 2, "free": 1}

# ── Index Configuration ───────────────────────────────────────────────────────
INDEX_CONFIG = {
    "settings": {
        "index": {
            "number_of_shards":   1,
            "number_of_replicas": 0,
            "refresh_interval":   "-1",     # disabled for bulk load; restored after
            "max_result_window":  50000,
        },

        # ── Analysis chain ────────────────────────────────────────────────────
        "analysis": {
            "filter": {
                "english_stop": {
                    "type":      "stop",
                    "stopwords": "_english_"
                },
                "english_stemmer": {
                    "type":     "stemmer",
                    "language": "english"
                },
                "product_synonyms": {
                    "type": "synonym",
                    "synonyms": [
                        # Electronics
                        "cap, capacitor",
                        "resistor, res",
                        "inductor, coil, choke",
                        "pcb, circuit board, printed circuit",
                        "ups, uninterruptible power supply",
                        "inverter, power converter",
                        "stabilizer, voltage regulator",
                        # Mechanical
                        "v belt, v-belt, vbelt",
                        "eot crane, overhead crane, bridge crane",
                        "compressor, air compressor",
                        "pump, pumping system",
                        # Food
                        "rice, paddy",
                        "basmati, long grain rice",
                        "oil, edible oil, cooking oil",
                        "atta, flour, wheat flour",
                        # Apparel
                        "t-shirt, tshirt, tee",
                        "lungi, dhoti",
                        # Construction
                        "plywood, ply",
                        "sealant, silicone, caulk",
                        "waterproof, waterproofing",
                        "roofing, roof sheet",
                        "epoxy, resin coating",
                        # Medical
                        "wheelchair, wheel chair",
                        "walker, mobility aid",
                    ]
                },
                "edge_ngram_filter": {
                    "type":     "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 20
                }
            },

            "analyzer": {
                # Full-text: stem + stop ─────────────────────────────────────
                "product_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter":    ["lowercase", "english_stop", "english_stemmer"]
                },
                # Synonym-aware (index time for productName + businessName) ──
                "product_synonym_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter":    ["lowercase", "english_stop", "product_synonyms", "english_stemmer"]
                },
                # Autocomplete: edge n-gram ────────────────────────────────
                "autocomplete_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter":    ["lowercase", "edge_ngram_filter"]
                },
                "autocomplete_search_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter":    ["lowercase"]
                }
            }
        }
    },

    # ── Field Mappings ────────────────────────────────────────────────────────
    "mappings": {
        "dynamic": "strict",
        "properties": {

            # ── Product fields ────────────────────────────────────────────────
            "productName": {
                "type":            "text",
                "analyzer":        "product_synonym_analyzer",
                "search_analyzer": "product_analyzer",
                "fields": {
                    "keyword":      {"type": "keyword", "ignore_above": 512},
                    "autocomplete": {
                        "type":            "text",
                        "analyzer":        "autocomplete_analyzer",
                        "search_analyzer": "autocomplete_search_analyzer"
                    }
                }
            },
            "productDescription": {
                "type":     "text",
                "analyzer": "product_analyzer",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 1024, "index": False}
                }
            },

            # ── Business profile fields ───────────────────────────────────────
            "businessName": {
                "type":            "text",
                "analyzer":        "product_synonym_analyzer",
                "search_analyzer": "product_analyzer",
                "fields": {
                    "keyword":      {"type": "keyword", "ignore_above": 512},
                    "autocomplete": {
                        "type":            "text",
                        "analyzer":        "autocomplete_analyzer",
                        "search_analyzer": "autocomplete_search_analyzer"
                    }
                }
            },
            "businessCountry":  {"type": "keyword"},
            # Full URL to company logo (stored only, not indexed)
            "businessLogo":     {"type": "keyword", "index": False},
            # Slug for profile URL e.g. pepagora.com/inelec-engineers-pvt-ltd
            "businessSubDomain":{"type": "keyword"},
            "userId":           {"type": "keyword"},

            # ── Tier: stored as keyword label + numeric rank ──────────────────
            # packageType: "global" | "scale" | "grow" | "free"
            "packageType":      {"type": "keyword"},
            # tierRank: 4=global, 3=scale, 2=grow, 1=free  → used in function_score
            "tierRank":         {"type": "integer"},

            # ── Categories: keyword array for facets + text for search ────────
            "categories": {
                "type":     "text",
                "analyzer": "product_analyzer",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256}
                }
            },
        }
    }
}


def create_index(recreate: bool = True):
    """Create (or recreate) the products_v2 index."""
    exists = es.indices.exists(index=INDEX_NAME)
    if exists:
        if recreate:
            print(f"⚠  Index '{INDEX_NAME}' exists — deleting …")
            es.indices.delete(index=INDEX_NAME)
            print("   Deleted.")
        else:
            print(f"✔  Index '{INDEX_NAME}' already exists. Skipping.")
            return

    print(f"Creating index '{INDEX_NAME}' …")
    es.indices.create(index=INDEX_NAME, body=INDEX_CONFIG)
    print(f"✔  Index '{INDEX_NAME}' created.\n")
    print("   Field layout:")
    print("   ┌─ productName        → text (synonym+stem) + keyword + autocomplete (ngram)")
    print("   ├─ productDescription → text (stem)")
    print("   ├─ businessName       → text (synonym+stem) + keyword + autocomplete (ngram)")
    print("   ├─ businessCountry    → keyword (filter)")
    print("   ├─ businessLogo       → keyword (stored, not indexed)")
    print("   ├─ businessSubDomain  → keyword (profile URL slug)")
    print("   ├─ packageType        → keyword  (global/scale/grow/free)")
    print("   ├─ tierRank           → integer  (4/3/2/1 — used for boosting)")
    print("   └─ categories         → text + keyword[]")


def restore_for_search():
    """Re-enable refresh after bulk load is complete."""
    print(f"Restoring '{INDEX_NAME}' for live search …")
    es.indices.put_settings(
        index=INDEX_NAME,
        body={"index": {"refresh_interval": "1s", "number_of_replicas": 0}}
    )
    es.indices.refresh(index=INDEX_NAME)
    print("✔  Ready for search.")


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--show" in args:
        print(json.dumps(INDEX_CONFIG, indent=2))
    elif "--restore" in args:
        restore_for_search()
    else:
        try:
            es.info()
        except Exception as e:
            print(f"❌ Cannot connect to Elasticsearch: {e}")
            sys.exit(1)
        create_index(recreate=True)
