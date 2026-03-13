"""
products_index.py
-----------------
Creates (or recreates) the 'products' index in Elasticsearch with:
  - Custom analyzers: english stemmer, synonym expansion, edge n-gram autocomplete
  - Optimal field mappings: text + keyword sub-fields with boosted productName
  - Index settings tuned for 100k-doc bulk load

Usage:
    python products_index.py                         # create / recreate
    python products_index.py --show                  # print mapping only
"""

import sys
import urllib3
from elasticsearch import Elasticsearch

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── Connection ───────────────────────────────────────────────────────────────
ES_HOST    = "https://127.0.0.1:9200"
ES_USER    = "elastic"
ES_PASS    = "dcJwpNq4m6wtDNpdbUhY"
INDEX_NAME = "products"

es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASS), verify_certs=False)

# ─── Index Configuration ─────────────────────────────────────────────────────
INDEX_CONFIG = {
    "settings": {
        # ── Performance: tuned for bulk load; restored after indexing ──
        "index": {
            "number_of_shards":   1,
            "number_of_replicas": 0,          # disable replica during bulk load
            "refresh_interval":   "-1",       # disable auto-refresh during bulk load
        },

        # ── Analysis ──
        "analysis": {
            "filter": {
                # Removes common English stop-words (the, is, for …)
                "english_stop": {
                    "type":      "stop",
                    "stopwords": "_english_"
                },
                # Reduces words to root form (capacitors → capacitor, industrial → industri)
                "english_stemmer": {
                    "type":     "stemmer",
                    "language": "english"
                },
                # Adds common e-commerce product synonyms so different terms
                # map to the same tokens at index + query time.
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
                        # Mechanical / industrial
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
                        # Medical / Health
                        "wheelchair, wheel chair",
                        "walker, mobility aid",
                    ]
                },
                # Edge n-gram for autocomplete: 'ind' matches 'industrial'
                "edge_ngram_filter": {
                    "type":     "edge_ngram",
                    "min_gram": 2,
                    "max_gram": 20
                }
            },

            "analyzer": {
                # ── Main full-text analyzer: stem + stop + lowercase ──
                "product_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer"
                    ]
                },
                # ── Synonym-aware: used for productName at index time ──
                "product_synonym_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "product_synonyms",   # expand synonyms
                        "english_stemmer"
                    ]
                },
                # ── Autocomplete: edge n-gram for starts-with search ──
                "autocomplete_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "edge_ngram_filter"
                    ]
                },
                # ── Standard search-time analyzer (no n-gram expansion) ──
                "autocomplete_search_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                }
            }
        }
    },

    # ─── Field Mappings ───────────────────────────────────────────────────────
    "mappings": {
        "dynamic": "strict",          # reject unknown fields (clean data only)
        "properties": {

            # MongoDB document ID (stored as keyword for filtering)
            "_id_orig": {
                "type": "keyword",
                "store": True
            },

            # ── Primary search field ──────────────────────────────────────
            "productName": {
                "type":     "text",
                "analyzer": "product_synonym_analyzer",  # synonyms at index time
                "search_analyzer": "product_analyzer",   # stem-only at query time
                "fields": {
                    # exact / sorting
                    "keyword": {
                        "type":         "keyword",
                        "ignore_above": 512
                    },
                    # autocomplete
                    "autocomplete": {
                        "type":           "text",
                        "analyzer":       "autocomplete_analyzer",
                        "search_analyzer": "autocomplete_search_analyzer"
                    }
                }
            },

            # ── Rich description: semantic search ─────────────────────────
            "productDescription": {
                "type":     "text",
                "analyzer": "product_analyzer",
                "fields": {
                    "keyword": {
                        "type":         "keyword",
                        "ignore_above": 1024,
                        "index":        False    # don't index keyword variant (too long)
                    }
                }
            },

            # ── Category: text for search + keyword for aggregations ──────
            "category": {
                "properties": {
                    "name": {
                        "type":     "text",
                        "analyzer": "product_analyzer",
                        "fields": {
                            "keyword": {
                                "type":         "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "subCategoryName": {
                        "type":     "text",
                        "analyzer": "product_analyzer",
                        "fields": {
                            "keyword": {
                                "type":         "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            },

            # ── Product Category ──────────────────────────────────────────
            "productCategory": {
                "properties": {
                    "_id": {
                        "type": "keyword"
                    },
                    "name": {
                        "type":     "text",
                        "analyzer": "product_analyzer",
                        "fields": {
                            "keyword": {
                                "type":         "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "uniqueId": {
                        "type": "keyword"
                    }
                }
            }
        }
    }
}


def create_index(recreate: bool = True):
    """Create or recreate the products index."""
    exists = es.indices.exists(index=INDEX_NAME)

    if exists:
        if recreate:
            print(f"⚠  Index '{INDEX_NAME}' exists. Deleting …")
            es.indices.delete(index=INDEX_NAME)
            print(f"   Deleted.")
        else:
            print(f"✔  Index '{INDEX_NAME}' already exists. Skipping creation.")
            return

    print(f"Creating index '{INDEX_NAME}' …")
    es.indices.create(index=INDEX_NAME, body=INDEX_CONFIG)
    print(f"✔  Index '{INDEX_NAME}' created successfully.\n")
    print("   Mapping summary:")
    print("   ┌─ productName          → text (synonym+stem) + keyword + autocomplete")
    print("   ├─ productDescription   → text (stem) + keyword (not indexed)")
    print("   ├─ category.name        → text (stem) + keyword")
    print("   ├─ category.subCategoryName → text (stem) + keyword")
    print("   ├─ productCategory.name → text (stem) + keyword")
    print("   ├─ productCategory._id  → keyword")
    print("   └─ productCategory.uniqueId → keyword")


def restore_index_for_search():
    """
    After bulk indexing, call this to re-enable refresh and replicas.
    Run: python products_index.py --restore
    """
    print(f"Restoring index '{INDEX_NAME}' settings for live search …")
    es.indices.put_settings(
        index=INDEX_NAME,
        body={
            "index": {
                "refresh_interval":   "1s",
                "number_of_replicas": 0     # single-node local — keep at 0
            }
        }
    )
    es.indices.refresh(index=INDEX_NAME)
    print("✔  Index is now live for search.")


if __name__ == "__main__":
    args = sys.argv[1:]

    if "--show" in args:
        import json
        print(json.dumps(INDEX_CONFIG, indent=2))
    elif "--restore" in args:
        restore_index_for_search()
    else:
        # Default: recreate the index
        try:
            es.info()  # connectivity check
        except Exception as e:
            print(f"❌ Cannot connect to Elasticsearch: {e}")
            sys.exit(1)

        create_index(recreate=True)
