import json
import os
import pandas as pd
from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer

# --- CONFIGURATION ---
OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = "admin"
OPENSEARCH_PASS = "K@ifShaheemj17"
INDEX_NAME = "pepagora_hierarchy"
MODEL_NAME = "all-MiniLM-L6-v2"
PRODUCTS_FILE = r'c:\\Kaif\Pepagora\pepagoraDb.liveproducts.json'
OUTPUT_FILE = r'c:\Kaif\Pepagora\mapped_products_with_accuracy.xlsx'

def map_products():
    print("Loading embedding model...")
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

    # To process only a subset for testing or large files
    # products = products[:100]  # Uncomment to test with first 100 products

    results = []
    total_products = len(products)
    print(f"Processing {total_products} products...")

    for i, product in enumerate(products):
        product_name = product.get('productName', '')
        product_description = product.get('productDescription', '')
        
        product_text = f"{product_name} {product_description}"

        # Generate embedding
        query_vector = model.encode(
            product_text,
            normalize_embeddings=True
        ).tolist()

        query = {
            "size": 1, # Only need the top 1 match
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

        if (i + 1) % 10 == 0 or (i + 1) == total_products:
            print(f"Processed {i + 1}/{total_products} products...")

    print(f"Saving results to {OUTPUT_FILE}...")
    df = pd.DataFrame(results)
    
    try:
        df.to_excel(OUTPUT_FILE, index=False)
        print("Done! Mapping complete.")
    except Exception as e:
        print(f"Error saving to Excel: {e}")
        csv_file = OUTPUT_FILE.replace('.xlsx', '.csv')
        print(f"Attempting to save to CSV instead: {csv_file}")
        df.to_csv(csv_file, index=False)
        print("Saved to CSV.")

if __name__ == "__main__":
    try:
        map_products()
    except Exception as e:
        print(f"An error occurred: {e}")
