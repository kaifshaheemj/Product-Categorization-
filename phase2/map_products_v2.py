import json
import os
import pandas as pd
from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer

# --- CONFIGURATION (PHASE 2) ---
OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = "admin"
OPENSEARCH_PASS = "K@ifShaheemj17"
INDEX_NAME = "pepagora_hierarchy_v2" # Using V2 index
MODEL_NAME = "all-mpnet-base-v2"      # Upgraded model
PRODUCTS_FILE = r'c:\Kaif\Pepagora\dataset\pepagoraDb.liveproducts.json'
OUTPUT_FILE = r'c:\Kaif\Pepagora\phase2\mapped_products_v2_50k.csv'

def map_products_v2():
    print(f"Loading embedding model {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)

    print("Connecting to OpenSearch...")
    client = OpenSearch(
        hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        http_compress=True
    )

    print(f"Loading products from {PRODUCTS_FILE}...")
    with open(PRODUCTS_FILE, 'r', encoding='utf-8') as f:
        products = json.load(f)

    results = []
    # Process the first 1000 for validation (or adjust as needed)
    total_to_process = len(products)
    print(f"Processing first {total_to_process} products...")

    for i in range(total_to_process):
        product = products[i]
        product_name = product.get('productName', '')
        product_description = product.get('productDescription', '')
        
        # Product text for query
        product_text = f"{product_name} {product_description}"

        # Generate embedding
        query_vector = model.encode(
            product_text,
            normalize_embeddings=True
        ).tolist()

        query = {
            "size": 1,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_vector,
                        "k": 1
                    }
                }
            }
        }

        response = client.search(index=INDEX_NAME, body=query)
        
        best_match = {
            "Product Name": product_name,
            "Product Description": product_description,
            "Main Category": "N/A",
            "Sub Category": "N/A",
            "Product Category": "N/A",
            "Accuracy": 0.0
        }

        if response["hits"]["hits"]:
            hit = response["hits"]["hits"][0]
            source = hit["_source"]
            best_match["Main Category"] = source.get('CategoryName', 'N/A')
            best_match["Sub Category"] = source.get('SubcategoryName', 'N/A')
            best_match["Product Category"] = source.get('ProductCategoryName', 'N/A')
            best_match["Accuracy"] = round(hit["_score"], 4)

        results.append(best_match)

        if (i + 1) % 10000 == 0:
            print(f"Processed {i + 1}/{total_to_process} products...")

    print(f"Saving results to {OUTPUT_FILE}...")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_FILE, index=False)
    print("Done! Phase 2 Mapping complete.")

if __name__ == "__main__":
    try:
        map_products_v2()
    except Exception as e:
        print(f"An error occurred: {e}")
