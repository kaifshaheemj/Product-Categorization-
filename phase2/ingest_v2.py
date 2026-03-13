import json
from opensearchpy import OpenSearch, helpers
from sentence_transformers import SentenceTransformer
import os

# --- CONFIGURATION (PHASE 2) ---
OPENSEARCH_HOST = 'localhost'
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASS = 'K@ifShaheemj17'
INDEX_NAME = 'pepagora_hierarchy_v2'  # New index for Phase 2
MODEL_NAME = 'all-mpnet-base-v2'      # Upgraded model (768 dimensions)
HIERARCHY_FILE = r'c:\Kaif\Pepagora\dataset\hierarchy.json'

def ingest_data_v2():
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
    
    # Define Index Mapping for Phase 2 (768 dimensions)
    index_body = {
        "settings": {
            "index.knn": True
        },
        "mappings": {
            "properties": {
                "CategoryId": { "type": "keyword" },
                "CategoryName": { "type": "keyword" },
                "SubcategoryId": { "type": "keyword" },
                "SubcategoryName": { "type": "keyword" },
                "ProductCategoryId": { "type": "keyword" },
                "ProductCategoryName": { "type": "keyword" },
                "structured_text": { "type": "text" },
                "embedding": {
                    "type": "knn_vector",
                    "dimension": 768, # Upgraded from 384
                    "method": {
                        "name": "hnsw",
                        "engine": "lucene",
                        "space_type": "l2",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 16
                        }
                    }
                }
            }
        }
    }

    if client.indices.exists(index=INDEX_NAME):
        print(f"Index {INDEX_NAME} already exists. Deleting...")
        client.indices.delete(index=INDEX_NAME)

    print(f"Creating index {INDEX_NAME}...")
    client.indices.create(index=INDEX_NAME, body=index_body)

    print(f"Loading embedding model {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)

    print(f"Loading data from {HIERARCHY_FILE}...")
    with open(HIERARCHY_FILE, 'r', encoding='utf-8') as f:
        hierarchy_data = json.load(f)

    print("Preparing documents for ingestion...")
    actions = []
    for i, item in enumerate(hierarchy_data):
        # Phase 2 improvement: Structured text for better context
        # Boosting Subcategory by repeating it
        structured_text = f"Main: {item['CategoryName']} | Sub: {item['SubcategoryName']} {item['SubcategoryName']} | Product: {item['ProductCategoryName']}"
        
        # Generate embedding
        embedding = model.encode(structured_text, normalize_embeddings=True).tolist()

        doc = {
            "_index": INDEX_NAME,
            "_id": item['ProductCategoryId'],
            "_source": {
                "CategoryId": item['CategoryId'],
                "CategoryName": item['CategoryName'],
                "SubcategoryId": item['SubcategoryId'],
                "SubcategoryName": item['SubcategoryName'],
                "ProductCategoryId": item['ProductCategoryId'],
                "ProductCategoryName": item['ProductCategoryName'],
                "structured_text": structured_text,
                "embedding": embedding
            }
        }
        actions.append(doc)

        if len(actions) >= 500:
            helpers.bulk(client, actions)
            print(f"Ingested {i + 1} documents...")
            actions = []

    if actions:
        helpers.bulk(client, actions)
        print(f"Ingested all {len(hierarchy_data)} documents.")

    print("Success!")

if __name__ == "__main__":
    try:
        ingest_data_v2()
    except Exception as e:
        print(f"An error occurred: {e}")
