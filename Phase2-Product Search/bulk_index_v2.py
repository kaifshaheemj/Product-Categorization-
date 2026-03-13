"""
bulk_index_v2.py
----------------
Ingests preprocessed_products.json into the 'products_v2' Elasticsearch index.

Key design decisions for latency / speed:
  - Reads the full business profiles JSON once into RAM (only 7 MB) to build a fast
    userId → companyLogo lookup dict
  - Streams the large preprocessed_products.json using ijson (incremental JSON parser)
    so it never loads 134 MB into RAM at once
  - Uses helpers.parallel_bulk (chunk=500, threads=4, queue=8) for maximum throughput
  - Tier rank (global=4, scale=3, grow=2, free=1) is stored as an integer for
    function_score queries (no runtime scripting needed)
  - Company logo URL is constructed as:
      https://www.pepagora.com/{companyLogo.src}
    if the logo src exists in business profiles; otherwise empty string

Usage:
    python bulk_index_v2.py                        # full index
    python bulk_index_v2.py --limit 500            # test with 500 docs
    python bulk_index_v2.py --skip-index-create    # reindex without schema recreate
"""

import sys
import json
import time
import urllib3

try:
    import ijson
    USE_IJSON = True
except ImportError:
    USE_IJSON = False
    print("⚠  ijson not found – falling back to full JSON load (may use ~400MB RAM)")

from elasticsearch import Elasticsearch, helpers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Config ────────────────────────────────────────────────────────────────────
ES_HOST    = "https://127.0.0.1:9200"
ES_USER    = "elastic"
ES_PASS    = "dcJwpNq4m6wtDNpdbUhY"
INDEX_NAME = "products_v2"

BASE_DIR          = r"C:\Kaif\Elasticsearch\Phase2-Product Search\pepagoraDb details"
PRODUCTS_JSON     = BASE_DIR + r"\preprocessed_products.json"
BIZPROFILES_JSON  = BASE_DIR + r"\pepagoraDb.businessprofiles.json"

PEPAGORA_BASE_URL = "https://www.pepagora.com/"

CHUNK_SIZE   = 500
THREAD_COUNT = 4
QUEUE_SIZE   = 8

TIER_RANK = {"global": 4, "scale": 3, "grow": 2, "free": 1}


# ── Step 1: Build logo lookup  ────────────────────────────────────────────────
def build_logo_lookup(biz_json_path: str) -> dict:
    """
    Read pepagoraDb.businessprofiles.json and return:
        { userId (createdBy.$oid): full_logo_url }
    Only profiles WITH a companyLogo.src are stored.
    """
    print("Loading business profiles for logo lookup …")
    with open(biz_json_path, encoding="utf-8") as f:
        profiles = json.load(f)

    lookup = {}
    for biz in profiles:
        uid = biz.get("createdBy", {})
        if isinstance(uid, dict):
            uid = uid.get("$oid", "")
        logo_obj = biz.get("companyLogo", {})
        logo_src = logo_obj.get("src", "") if isinstance(logo_obj, dict) else ""
        if logo_src:
            lookup[uid] = PEPAGORA_BASE_URL + logo_src
    print(f"  ✔  Logo entries loaded: {len(lookup):,}")
    return lookup


# ── Step 2: Document generator ────────────────────────────────────────────────
def doc_generator(products_path: str, logo_lookup: dict, limit: int = 0):
    """
    Lazily yields ES bulk action dicts from preprocessed_products.json.
    Each document gets:
      • tierRank (integer) derived from packageType
      • businessLogo URL from the logo lookup dict
    """
    count = 0

    if USE_IJSON:
        f = open(products_path, encoding="utf-8")
        items = ijson.items(f, "item")
    else:
        f = open(products_path, encoding="utf-8")
        items = iter(json.load(f))

    try:
        for row in items:
            pkg   = (row.get("packageType") or "free").lower()
            rank  = TIER_RANK.get(pkg, 1)
            uid   = row.get("userId", "")
            logo  = logo_lookup.get(uid, "")
            cats  = row.get("categories", [])
            if isinstance(cats, str):
                cats = [cats]

            yield {
                "_index": INDEX_NAME,
                "_id":    row["id"],
                "_source": {
                    "productName":        (row.get("productName")        or "").strip(),
                    "productDescription": (row.get("productDescription") or "").strip(),
                    "businessName":       (row.get("businessName")       or "").strip(),
                    "businessCountry":    (row.get("businessCountry")    or "").strip(),
                    "businessLogo":       logo,
                    "businessSubDomain":  (row.get("subDomain")          or "").strip(),
                    "userId":             uid,
                    "packageType":        pkg,
                    "tierRank":           rank,
                    "categories":         cats,
                }
            }
            count += 1
            if limit and count >= limit:
                break
    finally:
        f.close()


# ── Step 3: Main ─────────────────────────────────────────────────────────────
def main():
    args         = sys.argv[1:]
    skip_create  = "--skip-index-create" in args
    limit        = 0
    for a in args:
        if a.startswith("--limit="):
            limit = int(a.split("=")[1])
        elif a == "--limit" and args.index(a) + 1 < len(args):
            limit = int(args[args.index(a) + 1])

    # Connect
    es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASS), verify_certs=False)
    try:
        info = es.info()
        print(f"✔  Connected: {info['cluster_name']}  (ES {info['version']['number']})")
    except Exception as e:
        print(f"❌ Cannot connect: {e}")
        sys.exit(1)

    # Recreate index
    if not skip_create:
        import products_index_v2 as pi
        pi.create_index(recreate=True)
    else:
        print(f"⏭  Skipping index creation — using existing '{INDEX_NAME}'")

    # Disable refresh for bulk speed
    es.indices.put_settings(
        index=INDEX_NAME,
        body={"index": {"refresh_interval": "-1", "number_of_replicas": 0}}
    )

    # Build logo lookup (~7 MB, fast)
    logo_lookup = build_logo_lookup(BIZPROFILES_JSON)

    if limit:
        print(f"\nIndexing first {limit:,} documents (--limit mode) …")
    else:
        print(f"\nIndexing ALL documents …")
    print("─" * 60)

    success_count = 0
    failed_count  = 0
    start         = time.perf_counter()
    report_every  = 5_000

    for ok, info in helpers.parallel_bulk(
        es,
        doc_generator(PRODUCTS_JSON, logo_lookup, limit),
        chunk_size   = CHUNK_SIZE,
        thread_count = THREAD_COUNT,
        queue_size   = QUEUE_SIZE,
        raise_on_error=False,
    ):
        if ok:
            success_count += 1
        else:
            failed_count += 1
            act = info.get("index", {})
            print(f"  ⚠  Failed: {act.get('_id')} — {act.get('error', {}).get('reason', '?')}")

        done = success_count + failed_count
        now  = time.perf_counter()
        if done % report_every == 0:
            elapsed = now - start
            rate    = done / elapsed if elapsed > 0 else 0
            print(f"  {done:>7,} docs  @ {rate:,.0f} docs/s")

    elapsed_total = time.perf_counter() - start
    avg_rate      = success_count / elapsed_total if elapsed_total > 0 else 0

    print("─" * 60)
    print(f"\n✔  Indexing done in {elapsed_total:.1f}s")
    print(f"   Indexed : {success_count:,}")
    print(f"   Failed  : {failed_count:,}")
    print(f"   Avg rate: {avg_rate:,.0f} docs/s")

    # Restore and force merge
    print("\nRestoring index for live search …")
    es.indices.put_settings(
        index=INDEX_NAME,
        body={"index": {"refresh_interval": "1s", "number_of_replicas": 0}}
    )
    es.indices.refresh(index=INDEX_NAME)
    print("Force-merging to 1 segment (reduces query latency) …")
    es.indices.forcemerge(index=INDEX_NAME, max_num_segments=1)
    print("✔  Force merge done.")

    count_resp = es.count(index=INDEX_NAME)
    print(f"✔  Total docs verified: {count_resp['count']:,}")


if __name__ == "__main__":
    main()
