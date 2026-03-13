# from opensearchpy import OpenSearch
# from sentence_transformers import SentenceTransformer, CrossEncoder
# import numpy as np

# INDEX_NAME = "pepagora_hierarchy"

# embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
# reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")

# client = OpenSearch(
#     hosts=[{"host": "localhost", "port": 9200}],
#     http_auth=("admin", "K@ifShaheemj17"),
#     use_ssl=True,
#     verify_certs=False,
#     ssl_assert_hostname=False
# )


# def classify_product(product_name, description=""):

#     product_text = f"""
#     Product Name: {product_name}
#     Description: {description}
#     """

#     query_vector = embedding_model.encode(
#         product_text,
#         normalize_embeddings=True
#     ).tolist()

#     query = {
#         "size": 100,
#         "query": {
#             "bool": {
#                 "should": [

#                     {
#                         "knn": {
#                             "embedding": {
#                                 "vector": query_vector,
#                                 "k": 100
#                             }
#                         }
#                     },

#                     {
#                         "multi_match": {
#                             "query": product_text,
#                             "fields": [
#                                 "ProductCategoryName^10",
#                                 "SubcategoryName^6",
#                                 "CategoryName^3",
#                                 "category_path^4"
#                             ],
#                             "type": "most_fields",
#                             "fuzziness": "AUTO"
#                         }
#                     }
#                 ]
#             }
#         }
#     }

#     response = client.search(index=INDEX_NAME, body=query)

#     hits = response["hits"]["hits"]

#     pairs = []

#     for hit in hits:

#         source = hit["_source"]

#         category_text = f"""
#         Main Category: {source['CategoryName']}
#         Subcategory: {source['SubcategoryName']}
#         Product Category: {source['ProductCategoryName']}
#         """

#         pairs.append([product_text, category_text])

#     scores = reranker.predict(pairs)

#     best_index = int(np.argmax(scores))

#     best = hits[best_index]["_source"]

#     print("\nFinal Prediction\n")
#     print("Main Category :", best["CategoryName"])
#     print("Subcategory   :", best["SubcategoryName"])
#     print("Product Cat   :", best["ProductCategoryName"])

# if __name__ == "__main__":

#     # productName = "Industrial Power Systems Medium Voltage Capacitor Surface Mount Super Capacitor"

#     # productDescription = """
#     # Industrial Power Systems Medium Voltage Capacitor delivers reliable surface
#     # mount super capacitor performance for AC motor applications, featuring
#     # stainless steel construction and stable operation from -25°C to +55°C
#     # for industrial power factor correction and reactive power compensation needs.
#     # """
#     productDescription = "Industrial DC Voltage Relay 12VDC 24VDC provides precise under-voltage and over-voltage protection with sealed construction, PCB termination, and high-power contact load for reliable industrial applications.",
#     productName = "Industrial DC Voltage Relay 12VDC 24VDC Sealed High Power Protection",

#     classify_product(productName, productDescription)

from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer, CrossEncoder
import numpy as np

INDEX_NAME = "pepagora_hierarchy_v3"

# Keep your embedding model
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

# ✅ Better reranker: trained on NLI/semantic similarity, not web search
reranker = CrossEncoder("cross-encoder/stsb-roberta-base")
# Alternative if you want higher accuracy (heavier):
# reranker = CrossEncoder("cross-encoder/nli-deberta-v3-base")

client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "K@ifShaheemj17"),
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False
)


def build_product_text(product_name: str, description: str) -> str:
    """Clean, focused text for embedding — no label noise."""
    parts = [product_name.strip()]
    if description and description.strip():
        parts.append(description.strip())
    return " | ".join(parts)


def build_category_text(source: dict) -> str:
    """Structured category path for reranker comparison."""
    return (
        f"{source['CategoryName']} > "
        f"{source['SubcategoryName']}"
    )


def classify_product(product_name: str, description: str = "") -> dict:

    # ✅ Fix: clean text only — no f-string label pollution
    product_text = build_product_text(product_name, description)

    query_vector = embedding_model.encode(
        product_text,
        normalize_embeddings=True
    ).tolist()

    # ✅ Fix: removed collapse to avoid error when .keyword field is missing
    query = {
        "size": 50,   
        "query": {
            "bool": {
                "should": [
                    {
                        "knn": {
                            "embedding": {
                                "vector": query_vector,
                                "k": 50
                            }
                        }
                    },
                    {
                        "multi_match": {
                            "query": product_text,   
                            "fields": [
                                "SubcategoryName^10",
                                "CategoryName^8",
                                "category_path^4",
                                "ProductCategoryName^1"
                            ],
                            "type": "best_fields",
                            "fuzziness": "AUTO",
                            "boost": 0.1   
                        }
                    }
                ]
            }
        }
    }

    response = client.search(index=INDEX_NAME, body=query)
    hits = response["hits"]["hits"]

    if not hits:
        return {"error": "No candidates found"}

    # Build reranker pairs: (product_text, category_path)
    pairs = []
    for hit in hits:
        category_text = build_category_text(hit["_source"])
        pairs.append([product_text, category_text])

    # ✅ Reranker scores
    scores = reranker.predict(pairs)

    # ✅ Add: get top-3, not just top-1 — useful for confidence checking
    top_indices = np.argsort(scores)[::-1][:3]

    best = hits[top_indices[0]]["_source"]
    best_score = float(scores[top_indices[0]])

    result = {
        "main_category":    best["CategoryName"],
        "subcategory":      best["SubcategoryName"],
        "product_category": best["ProductCategoryName"],
        "confidence":       round(best_score, 4),
        "top_3": [
            {
                "main":       hits[i]["_source"]["CategoryName"],
                "sub":        hits[i]["_source"]["SubcategoryName"],
                "product":    hits[i]["_source"]["ProductCategoryName"],
                "score":      round(float(scores[i]), 4)
            }
            for i in top_indices
        ]
    }

    # ✅ Add: confidence warning for low-confidence predictions
    if best_score < 0.5:
        result["warning"] = "Low confidence — consider human review"

    print("\n=== Final Prediction ===")
    print(f"Main Category : {result['main_category']}")
    print(f"Subcategory   : {result['subcategory']}")
    print(f"Product Cat   : {result['product_category']}")
    print(f"Confidence    : {result['confidence']}")

    if "warning" in result:
        print(f"⚠️  {result['warning']}")

    print("\n--- Top 3 Candidates ---")
    for i, candidate in enumerate(result["top_3"], 1):
        print(f"{i}. {candidate['main']} > {candidate['sub']} > {candidate['product']}  (score: {candidate['score']})")

    return result


if __name__ == "__main__":

    # ✅ Fix: removed trailing commas (were making these tuples!)
    productName = "Industrial DC Voltage Relay 12VDC 24VDC Sealed High Power Protection"
    productDescription = "Industrial DC Voltage Relay 12VDC 24VDC provides precise under-voltage and over-voltage protection with sealed construction, PCB termination, and high-power contact load for reliable industrial applications."

    classify_product(productName, productDescription)