import json
from opensearchpy import OpenSearch, helpers
from sentence_transformers import SentenceTransformer
import os

# --- CONFIGURATION ---
# Replace these with your actual OpenSearch connection details
OPENSEARCH_HOST = 'localhost'
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASS = 'K@ifShaheemj17'
INDEX_NAME = 'pepagora_hierarchy'
MODEL_NAME = 'all-MiniLM-L6-v2'  # 384 dimensions
HIERARCHY_FILE = r'c:\\Kaif\\Pepagora\\hierarchy.json'

def ingest_data():
    # 1. Initialize OpenSearch Client
    # client = OpenSearch(
    #     hosts=[{
    #         "host": OPENSEARCH_HOST,
    #         "port": OPENSEARCH_PORT
    #     }],
    #     http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    #     use_ssl=False,
    #     verify_certs=False
    # )
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
    print(client.info())
    print("Connected to OpenSearch")

    # 2. Define Index Mapping
    index_body = {
        "settings": {
            "index.knn": True
        },
        "mappings": {
            "properties": {
                "CategoryId": { "type": "keyword" },
                "CategoryName": { "type": "text" },
                "SubcategoryId": { "type": "keyword" },
                "SubcategoryName": { "type": "text" },
                "ProductCategoryId": { "type": "keyword" },
                "ProductCategoryName": { "type": "text" },
                "category_path": {"type": "text"},
                "category_text": { "type": "text" },
                "embedding": {
                    "type": "knn_vector",
                    "dimension": 384,
                    "method": {
                        "name": "hnsw",
                        "engine": "lucene",
                        "space_type": "cosinesimil",
                        "parameters": {
                            "ef_construction": 256,
                            "m": 32
                        }
                    }
                }
            }
        }
    }

    # 3. Create Index
    if client.indices.exists(index=INDEX_NAME):
        print(f"Index {INDEX_NAME} already exists. Deleting...")
        client.indices.delete(index=INDEX_NAME)

    print(f"Creating index {INDEX_NAME}...")
    client.indices.create(
        index=INDEX_NAME,
        body=index_body
    )
    print("Index created successfully!")
    print("Index info:", client.indices.get(index=INDEX_NAME))
    # 4. Load Model and Data
    print(f"Loading embedding model {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)

    print(f"Loading data from {HIERARCHY_FILE}...")
    with open(HIERARCHY_FILE, 'r', encoding='utf-8') as f:
        hierarchy_data = json.load(f)

    # 5. Prepare Actions for Bulk Ingestion
    print("Preparing documents for ingestion...")
    actions = []
    for i, item in enumerate(hierarchy_data):
        # Combine hierarchy into a single string for semantic mapping
        # category_text = f"{item['CategoryName']} {item['SubcategoryName']} {item['ProductCategoryName']}"
        category_text = f"""
            Main Category: {item['CategoryName']}
            Subcategory: {item['SubcategoryName']}
            Product Category: {item['ProductCategoryName']}
            """
        # Generate embedding
        # embedding = model.encode(category_text, normalize_embeddings=True).tolist()
        embedding = model.encode(category_text)

        doc = {
            "_index": INDEX_NAME,
            "_id": item['ProductCategoryId'], # Use Product Category ID as unique doc ID
            "_source": {
                "CategoryId": item['CategoryId'],
                "CategoryName": item['CategoryName'],
                "SubcategoryId": item['SubcategoryId'],
                "SubcategoryName": item['SubcategoryName'],
                "ProductCategoryId": item['ProductCategoryId'],
                "ProductCategoryName": item['ProductCategoryName'],
                "category_text": category_text,
                "category_path": category_path,
                "embedding": embedding
            }
        }
        actions.append(doc)

        # Batch ingestion every 500 documents
        if len(actions) >= 500:
            helpers.bulk(client, actions)
            print(f"Ingested {i + 1} documents...")
            actions = []

    # Final batch
    if actions:
        helpers.bulk(client, actions)
        print(f"Ingested all {len(hierarchy_data)} documents.")

    print("Success!")

if __name__ == "__main__":
    try:
        ingest_data()
    except Exception as e:
        print(f"An error occurred: {e}")
