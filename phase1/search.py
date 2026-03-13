# import __main__
# from opensearchpy import OpenSearch
# from sentence_transformers import SentenceTransformer

# OPENSEARCH_HOST = "localhost"
# OPENSEARCH_PORT = 9200
# OPENSEARCH_USER = "admin"
# OPENSEARCH_PASS = "K@ifShaheemj17"
# INDEX_NAME = "pepagora_hierarchy"

# model = SentenceTransformer("all-MiniLM-L6-v2")

# client = OpenSearch(
#     hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
#     http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
#     use_ssl=True,
#     verify_certs=False,
#     ssl_assert_hostname=False,
#     ssl_show_warn=False,
#     http_compress=True
#     )

# def search_product(product_name, description=""):

#     product_text = f"{product_name} {description}"

#     # Generate embedding
#     query_vector = model.encode(
#         product_text,
#         normalize_embeddings=True
#     ).tolist()

#     query = {
#         "size": 5,
#         "query": {
#             "knn": {
#                 "embedding": {
#                     "vector": query_vector,
#                     "k": 5
#                 }
#             }
#         }
#     }

#     response = client.search(index=INDEX_NAME, body=query)

#     print("\nTop 5 Matching Categories:\n")

#     for hit in response["hits"]["hits"]:
#         source = hit["_source"]
#         score = hit["_score"]

#         print("Category Group:")
#         print(f"Main Category : {source['CategoryName']}")
#         print(f"Subcategory   : {source['SubcategoryName']}")
#         print(f"Product Cat   : {source['ProductCategoryName']}")
#         print(f"Accuracy Score: {round(score,4)}")
#         print("-"*40)


# if __name__ == "__main__":
#     productName = "Industrial Power Systems Medium Voltage Capacitor Surface Mount Super Capacitor"
#     productDescription = "Industrial Power Systems Medium Voltage Capacitor delivers reliable surface mount super capacitor performance for AC motor applications, featuring stainless steel construction and stable operation from -25°C to +55°C for industrial power factor correction and reactive power compensation needs."

#     search_product(productName, productDescription)

from opensearchpy import OpenSearch
from sentence_transformers import SentenceTransformer

OPENSEARCH_HOST = "localhost"
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = "admin"
OPENSEARCH_PASS = "K@ifShaheemj17"
INDEX_NAME = "pepagora_hierarchy"

model = SentenceTransformer("all-MiniLM-L6-v2")

client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
    http_compress=True
)

def search_product(product_name, description=""):

    product_text = f"{product_name} {description}"

    # Generate embedding
    query_vector = model.encode(
        product_text,
        normalize_embeddings=True
    ).tolist()

    query = {
        "size": 5,
        "query": {
            "bool": {
                "should": [

                    # Vector Search
                    {
                        "knn": {
                            "embedding": {
                                "vector": query_vector,
                                "k": 5
                            }
                        }
                    },

                    # Keyword Search
                    {
                        "multi_match": {
                            "query": product_text,
                            "fields": [
                                "ProductCategoryName^8",
                                "SubcategoryName^5",
                                "CategoryName^3",
                                "description^2"
                            ],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    }

                ]
            }
        }
    }

    response = client.search(index=INDEX_NAME, body=query)

    print("\nTop 5 Matching Categories:\n")

    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        score = hit["_score"]

        print("Category Group:")
        print(f"Main Category : {source['CategoryName']}")
        print(f"Subcategory   : {source['SubcategoryName']}")
        print(f"Product Cat   : {source['ProductCategoryName']}")
        print(f"Hybrid Score  : {round(score,4)}")
        print("-"*40)


if __name__ == "__main__":

    # productName = "Industrial Power Systems Medium Voltage Capacitor Surface Mount Super Capacitor"

    # productDescription = """
    # Industrial Power Systems Medium Voltage Capacitor delivers reliable surface
    # mount super capacitor performance for AC motor applications, featuring
    # stainless steel construction and stable operation from -25°C to +55°C
    # for industrial power factor correction and reactive power compensation needs.
    # """
    product_name = "Zip Lock Pouch Machines,heavy duty zip lock pouches for industrial packaging and storage"
    product_description = "Heavy duty zip lock pouches for industrial packaging and storage provide superior durability and reliable sealing. These premium pouches protect components, organize inventory, and secure products across multiple industries with consistent performance."

    search_product(product_name, product_description)