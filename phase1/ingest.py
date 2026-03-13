import json
from opensearchpy import OpenSearch, helpers
from sentence_transformers import SentenceTransformer

# --- CONFIGURATION ---
OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = "admin"
OPENSEARCH_PASS = "K@ifShaheemj17"

INDEX_NAME = "pepagora_hierarchy_v3"
MODEL_NAME = "all-MiniLM-L6-v2"  # 384 dimension embeddings

HIERARCHY_FILE = r"c:\Kaif\Pepagora\dataset\hierarchy.json"


def ingest_data():

    # 1️⃣ Connect to OpenSearch
    print("Connecting to OpenSearch...")

    client = OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        http_compress=True
    )

    print(client.info())
    print("Connected to OpenSearch")

    # 2️⃣ Index Mapping
    index_body = {
        "settings": {
            "index": {
                "knn": True
            }
        },
        "mappings": {
            "properties": {

                "CategoryId": {"type": "keyword"},
                "CategoryName": {"type": "text"},

                "SubcategoryId": {"type": "keyword"},
                "SubcategoryName": {"type": "text"},

                "ProductCategoryId": {"type": "keyword"},
                "ProductCategoryName": {"type": "text"},

                "category_path": {"type": "text"},
                "category_text": {"type": "text"},

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

    # 3️⃣ Recreate Index
    if client.indices.exists(index=INDEX_NAME):
        print(f"Index {INDEX_NAME} exists. Deleting...")
        client.indices.delete(index=INDEX_NAME)

    print(f"Creating index {INDEX_NAME}...")
    client.indices.create(index=INDEX_NAME, body=index_body)

    print("Index created successfully!")

    # 4️⃣ Load Embedding Model
    print(f"Loading embedding model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)

    # 5️⃣ Load Hierarchy Data
    print(f"Loading hierarchy data from {HIERARCHY_FILE}")

    with open(HIERARCHY_FILE, "r", encoding="utf-8") as f:
        hierarchy_data = json.load(f)

    print(f"Total records: {len(hierarchy_data)}")

    # 6️⃣ Prepare Batch Ingestion
    batch_texts = []
    batch_docs = []

    for i, item in enumerate(hierarchy_data):

        category_text = f"""
        Main Category: {item['CategoryName']}
        Subcategory: {item['SubcategoryName']}
        Product Category: {item['ProductCategoryName']}
        """

        category_path = f"{item['CategoryName']} > {item['SubcategoryName']} > {item['ProductCategoryName']}"

        batch_texts.append(category_text)

        batch_docs.append({
            "_index": INDEX_NAME,
            "_id": item["ProductCategoryId"],
            "_source": {

                "CategoryId": item["CategoryId"],
                "CategoryName": item["CategoryName"],

                "SubcategoryId": item["SubcategoryId"],
                "SubcategoryName": item["SubcategoryName"],

                "ProductCategoryId": item["ProductCategoryId"],
                "ProductCategoryName": item["ProductCategoryName"],

                "category_text": category_text,
                "category_path": category_path
            }
        })

        # 7️⃣ Batch Embedding + Bulk Insert
        if len(batch_docs) == 256:

            embeddings = model.encode(
                batch_texts,
                normalize_embeddings=True
            )

            for j, emb in enumerate(embeddings):
                batch_docs[j]["_source"]["embedding"] = emb.tolist()

            helpers.bulk(client, batch_docs)

            print(f"Ingested {i + 1} documents")

            batch_docs = []
            batch_texts = []

    # 8️⃣ Insert Remaining Documents
    if batch_docs:

        embeddings = model.encode(
            batch_texts,
            normalize_embeddings=True
        )

        for j, emb in enumerate(embeddings):
            batch_docs[j]["_source"]["embedding"] = emb.tolist()

        helpers.bulk(client, batch_docs)

        print("Final batch ingested")

    print("All documents indexed successfully!")


if __name__ == "__main__":

    try:
        ingest_data()
    except Exception as e:
        print("Error:", e)