from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer, CrossEncoder
import numpy as np

OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = "admin"
OPENSEARCH_PASS = "K@ifShaheemj17"
INDEX_NAME = "pepagora_hierarchy"

# Embedding model
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

# Cross encoder for reranking
reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
    http_compress=True
)


# def search_product(product_name, description=""):

#     product_text = f"{product_name} {description}"

#     # Generate embedding
#     query_vector = embedding_model.encode(
#         product_text,
#         normalize_embeddings=True
#     ).tolist()

#     # Hybrid search query
#     query = {
#         "size": 10,
#         "query": {
#             "bool": {
#                 "should": [

#                     # Vector similarity search
#                     {
#                         "knn": {
#                             "embedding": {
#                                 "vector": query_vector,
#                                 "k": 10
#                             }
#                         }
#                     },

#                     # Keyword BM25 search
#                     {
#                         "multi_match": {
#                             "query": product_text,
#                             "fields": [
#                                 "ProductCategoryName^8",
#                                 "SubcategoryName^5",
#                                 "CategoryName^3"
#                             ],
#                             "type": "best_fields",
#                             "fuzziness": "AUTO"
#                         }
#                     },

#                     # Phrase match boost
#                     {
#                         "match_phrase": {
#                             "SubcategoryName": {
#                                 "query": product_text,
#                                 "boost": 10
#                             }
#                         }
#                     }

#                 ]
#             }
#         }
#     }

#     response = client.search(index=INDEX_NAME, body=query)

#     hits = response["hits"]["hits"]

#     print("\nInitial Hybrid Search Results:\n")

#     for hit in hits:
#         source = hit["_source"]
#         score = hit["_score"]

#         print("Category Group:")
#         print(f"Main Category : {source['CategoryName']}")
#         print(f"Subcategory   : {source['SubcategoryName']}")
#         print(f"Product Cat   : {source['ProductCategoryName']}")
#         print(f"Hybrid Score  : {round(score,4)}")
#         print("-"*40)

#     # -----------------------------
#     # BERT RERANKING
#     # -----------------------------

#     pairs = []

#     for hit in hits:
#         source = hit["_source"]

#         category_text = (
#             source["CategoryName"] + " " +
#             source["SubcategoryName"] + " " +
#             source["ProductCategoryName"]
#         )

#         pairs.append([product_text, category_text])

#     scores = reranker.predict(pairs)

#     best_index = int(np.argmax(scores))
#     best_hit = hits[best_index]["_source"]

#     print("\nFinal Predicted Category (After BERT Reranking):\n")

#     print(f"Main Category : {best_hit['CategoryName']}")
#     print(f"Subcategory   : {best_hit['SubcategoryName']}")
#     print(f"Product Cat   : {best_hit['ProductCategoryName']}")
from tqdm import tqdm
import json
import os

def search_product(product_name, description=""):
    product_text = f"Product: {product_name}\nDescription: {description}"

    query_vector = embedding_model.encode(
        product_text,
        normalize_embeddings=True
    ).tolist()

    query = {
        "size": 50,
        "query": {
            "bool": {
                "should": [
                    {"knn": {"embedding": {"vector": query_vector, "k": 50}}},
                    {
                        "multi_match": {
                            "query": product_text,
                            "fields": ["ProductCategoryName^10", "SubcategoryName^6", "CategoryName^3"],
                            "type": "most_fields",
                            "fuzziness": "AUTO",
                            "boost": 2
                        }
                    },
                    {"match_phrase": {"ProductCategoryName": {"query": product_name, "boost": 15}}},
                    {"match_phrase": {"SubcategoryName": {"query": product_name, "boost": 10}}}
                ]
            }
        }
    }

    try:
        response = client.search(index=INDEX_NAME, body=query)
        hits = response["hits"]["hits"]
        if not hits:
            return None

        pairs = []
        for hit in hits:
            source = hit["_source"]
            category_text = f"Main Category: {source['CategoryName']}\nSubcategory: {source['SubcategoryName']}\nProduct Category: {source['ProductCategoryName']}"
            pairs.append([product_text, category_text])

        scores = reranker.predict(pairs, batch_size=32, show_progress_bar=False)
        best_index = int(np.argmax(scores))
        return hits[best_index]["_source"]
    except Exception as e:
        print(f"Error searching for {product_name}: {e}")
        return None

def process_dataset(input_json, output_csv, limit=None):
    print(f"Loading dataset from {input_json}...")
    with open(input_json, 'r', encoding='utf-8') as f:
        products = json.load(f)

    if limit:
        products = products[:limit]
    
    results = []
    print(f"Processing {len(products)} products...")
    
    for item in tqdm(products):
        name = item.get('productName', 'N/A')
        desc = item.get('productDescription', '')
        
        best_hit = search_product(name, desc)
        
        if best_hit:
            results.append({
                "Product Name": name,
                "Product Description": desc,
                "Main Category": best_hit['CategoryName'],
                "Sub Category": best_hit['SubcategoryName'],
                "Product Category": best_hit['ProductCategoryName']
            })
        else:
            results.append({
                "Product Name": name,
                "Product Description": desc,
                "Main Category": "N/A",
                "Sub Category": "N/A",
                "Product Category": "N/A"
            })

    print(f"Saving results to {output_csv}...")
    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False)
    print("Done!")

if __name__ == "__main__":
    import pandas as pd
    input_file = r"c:\Kaif\Pepagora\dataset\pepagoraDb.liveproducts.json"
    output_file = r"c:\Kaif\Pepagora\phase1\bert_mapped_products.csv"
    
    # You can set limit=None to process everything
    process_dataset(input_file, output_file)
