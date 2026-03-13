"""
bulk_index.py
-------------
Streams pepagoraDb.liveproducts.csv into Elasticsearch using parallel_bulk.
Memory-efficient: CSV is read row-by-row, never fully in RAM.

Usage:
    python bulk_index.py                            # uses default CSV path
    python bulk_index.py path/to/file.csv           # custom CSV path
    python bulk_index.py --skip-index-create        # don't recreate index
"""

import csv
import sys
import time
import urllib3

from elasticsearch import Elasticsearch, helpers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── Config ───────────────────────────────────────────────────────────────────
ES_HOST    = "https://127.0.0.1:9201"
ES_USER    = "elastic"
ES_PASS    = "dcJwpNq4m6wtDNpdbUhY"
INDEX_NAME = "products"

DEFAULT_CSV = r"C:\Kaif\Elasticsearch\pepagoraDb.liveproducts.csv"

CHUNK_SIZE   = 1000   # docs per bulk request
THREAD_COUNT = 4      # parallel threads
QUEUE_SIZE   = 8      # buffer of chunks in queue


# ─── Document Generator ───────────────────────────────────────────────────────
def doc_generator(csv_path: str):
    """
    Lazily reads the CSV and yields ES bulk action dicts.
    Uses the MongoDB _id from the dataset as the ES document _id.
    """
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc_id = row.get("_id", "").strip()
            yield {
                "_index": INDEX_NAME,
                "_id":    doc_id,          # use existing Mongo _id → avoids duplicates on re-run
                "_source": {
                    "_id_orig": doc_id,
                    "productName":        row.get("productName", "").strip(),
                    "productDescription": row.get("productDescription", "").strip(),
                    "category": {
                        "name":            row.get("category.name", "").strip(),
                        "subCategoryName": row.get("subCategory.name", "").strip(),
                    },
                    "productCategory": {
                        "_id":      row.get("productCategory._id", "").strip(),
                        "name":     row.get("productCategory.name", "").strip(),
                        "uniqueId": row.get("productCategory.uniqueId", "").strip(),
                    }
                }
            }


def count_rows(csv_path: str) -> int:
    """Count total rows for progress display (fast line count)."""
    with open(csv_path, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1   # subtract header


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]
    skip_create = "--skip-index-create" in args
    args = [a for a in args if not a.startswith("--")]
    csv_path = args[0] if args else DEFAULT_CSV

    # ── Connect ──────────────────────────────────────────────────────────────
    es = Elasticsearch(ES_HOST, basic_auth=(ES_USER, ES_PASS), verify_certs=False)
    try:
        info = es.info()
        print(f"✔ Connected to cluster: {info['cluster_name']}  (ES {info['version']['number']})")
    except Exception as e:
        print(f"❌ Cannot connect: {e}")
        sys.exit(1)

    # ── (Re)create index ─────────────────────────────────────────────────────
    if not skip_create:
        import products_index as pi
        pi.create_index(recreate=True)
    else:
        print(f"⏭  Skipping index creation — using existing '{INDEX_NAME}'")

    # ── Count rows ───────────────────────────────────────────────────────────
    print(f"\nCounting rows in CSV …")
    total = count_rows(csv_path)
    print(f"  Found {total:,} products to index.\n")

    # ── Disable refresh + replicas for speed ─────────────────────────────────
    # (already set in index creation, but keep safe for --skip-index-create)
    es.indices.put_settings(
        index=INDEX_NAME,
        body={"index": {"refresh_interval": "-1", "number_of_replicas": 0}}
    )

    # ── Parallel Bulk Index ───────────────────────────────────────────────────
    print(f"Indexing {total:,} documents  (chunk={CHUNK_SIZE}, threads={THREAD_COUNT}) …")
    print("─" * 60)

    success_count = 0
    failed_count  = 0
    start         = time.perf_counter()
    last_report   = start
    report_every  = 5_000   # print progress every 5k docs

    for ok, info in helpers.parallel_bulk(
        es,
        doc_generator(csv_path),
        chunk_size    = CHUNK_SIZE,
        thread_count  = THREAD_COUNT,
        queue_size    = QUEUE_SIZE,
        raise_on_error= False,
    ):
        if ok:
            success_count += 1
        else:
            failed_count += 1
            action = info.get("index", {})
            print(f"  ⚠  Failed doc: {action.get('_id')} — {action.get('error', {}).get('reason', '?')}")

        # ── Progress print ────────────────────────────────────────────────
        done = success_count + failed_count
        now  = time.perf_counter()
        if done % report_every == 0 or done == total:
            elapsed = now - start
            rate    = done / elapsed if elapsed > 0 else 0
            pct     = done / total * 100
            eta_s   = (total - done) / rate if rate > 0 else 0
            print(
                f"  [{pct:5.1f}%]  {done:>7,} / {total:,}  "
                f"@ {rate:,.0f} docs/s  "
                f"ETA: {eta_s:.0f}s"
            )
            last_report = now

    elapsed_total = time.perf_counter() - start
    avg_rate      = success_count / elapsed_total if elapsed_total > 0 else 0

    print("─" * 60)
    print(f"\n✔ Indexing complete in {elapsed_total:.1f}s")
    print(f"  Indexed:  {success_count:,} docs")
    print(f"  Failed:   {failed_count:,} docs")
    print(f"  Avg rate: {avg_rate:,.0f} docs/sec")

    # ── Re-enable refresh + force merge ──────────────────────────────────────
    print(f"\nRestoring index settings for live search …")
    es.indices.put_settings(
        index=INDEX_NAME,
        body={"index": {"refresh_interval": "1s", "number_of_replicas": 0}}
    )
    es.indices.refresh(index=INDEX_NAME)

    # Merge shards into 1 segment for optimal read performance
    print("Running force merge (this may take ~30s) …")
    es.indices.forcemerge(index=INDEX_NAME, max_num_segments=1)
    print("✔ Force merge done.\n")

    # ── Final count check ─────────────────────────────────────────────────────
    count_resp = es.count(index=INDEX_NAME)
    print(f"✔ Verified docs in index: {count_resp['count']:,}")


if __name__ == "__main__":
    main()
