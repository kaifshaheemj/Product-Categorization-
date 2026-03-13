import csv
import json
import os
import time

INPUT_FILE  = r"C:\Kaif\Elasticsearch\pepagoraDb.liveproducts.csv"
OUTPUT_FILE = r"C:\Kaif\Elasticsearch\pepagoraDb.liveproducts.json"

def csv_to_json(input_path, output_path):
    products = []

    print(f"Reading: {input_path}")
    start = time.perf_counter()

    with open(input_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Build a clean nested document
            doc = {
                "_id": row.get("_id", "").strip(),
                "productName": row.get("productName", "").strip(),
                "productDescription": row.get("productDescription", "").strip(),
                "category": {
                    "name": row.get("category.name", "").strip(),
                    "subCategoryName": row.get("subCategory.name", "").strip(),
                },
                "productCategory": {
                    "_id":      row.get("productCategory._id", "").strip(),
                    "name":     row.get("productCategory.name", "").strip(),
                    "uniqueId": row.get("productCategory.uniqueId", "").strip(),
                }
            }
            products.append(doc)

    elapsed_read = time.perf_counter() - start
    print(f"  ✔ Read {len(products):,} products in {elapsed_read:.2f}s")

    print(f"Writing: {output_path}")
    write_start = time.perf_counter()

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

    elapsed_write = time.perf_counter() - write_start
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  ✔ Written in {elapsed_write:.2f}s  |  File size: {size_mb:.1f} MB")
    print(f"\nDone! JSON saved to: {output_path}")

if __name__ == "__main__":
    csv_to_json(INPUT_FILE, OUTPUT_FILE)
