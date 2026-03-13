import json
from elasticsearch import Elasticsearch, helpers
import sys

# Connect to the local Elasticsearch instance
try:
    # Modern Elasticsearch 8.x has HTTPS and security enabled by default
    # If your local cluster uses HTTPS but you lack the CA certificate, set verify_certs=False
    es = Elasticsearch(
        "https://127.0.0.1:9201", 
        basic_auth=("elastic", "dcJwpNq4m6wtDNpdbUhY"),
        verify_certs=False, 
        # basic_auth=("elastic", "your_password_here") # Uncomment and set password if authentication is enabled
    )
    # Give a warning disable if using verify_certs=False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Test connection. If HTTP 401 Unauthorized is returned, ping() might still return False but we should proceed and let authorization error later, or give a distinct warning
    try:
        info = es.info()
        print(f"Connected to Elasticsearch cluster: {info.get('cluster_name')}")
    except Exception as ping_err:
        print(f"Warning: Connection check failed. It may be due to missing authentication (username/password).")
        print(f"Details: {ping_err}")
        print(f"If your Elasticsearch requires a password, please edit this script to uncomment and update the 'basic_auth' line.")
        # We don't exit here, we let the actual operations try and fail naturally if it's a hard error
        
except Exception as e:
    print(f"Failed to connect to Elasticsearch: {e}")
    sys.exit(1)

INDEX_NAME = 'drugs_index'

def load_data(filepath):
    """Loads JSON data to Elasticsearch."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return

    # Prepare data for bulk indexing
    actions = []
    for drug_name, details in data.items():
        doc = {"drug_name": drug_name}
        doc.update(details)
        
        # Ensure dosage_forms is a string if it's a list for consistency 
        if "dosage_forms" in doc and isinstance(doc["dosage_forms"], list):
            doc["dosage_forms"] = ", ".join(doc["dosage_forms"])
            
        action = {
            "_index": INDEX_NAME,
            "_source": doc
        }
        actions.append(action)

    print("Checking if index exists...")
    # Create index if it doesn't exist
    if es.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists.")
        response = input("Do you want to delete and recreate the index? (y/n): ")
        if response.lower() == 'y':
            es.indices.delete(index=INDEX_NAME)
            es.indices.create(index=INDEX_NAME)
            print(f"Index '{INDEX_NAME}' recreated.")
    else:
        print(f"Creating index '{INDEX_NAME}'...")
        es.indices.create(index=INDEX_NAME)

    print(f"Indexing {len(actions)} documents into Elasticsearch...")
    try:
        success, failed = helpers.bulk(es, actions)
        print(f"Successfully indexed {success} documents.")
        if failed:
            print(f"Failed to index {len(failed)} documents.")
    except Exception as e:
        print(f"Bulk indexing error: {e}")

def search_database(query_string, size=10):
    """Searches the Elasticsearch database."""
    print(f"\nSearching '{INDEX_NAME}' for: '{query_string}'...\n")
    
    # Multi-match query across multiple fields, prioritizing drug_name
    query = {
        "query": {
            "multi_match": {
                "query": query_string,
                "fields": [
                    "drug_name^4", 
                    "indications^3", 
                    "adverse_effects^2", 
                    "contraindications", 
                    "dose"
                ],
                "fuzziness": "AUTO"
            }
        },
        "size": size
    }
    
    try:
        # Perform the search
        response = es.search(index=INDEX_NAME, body=query)
        hits = response['hits']['hits']
        total = response['hits']['total']['value'] if type(response['hits']['total']) == dict else response['hits']['total']
        
        print(f"Found {total} total matches. Showing top {len(hits)} results:")
        print("-" * 60)
        
        for i, hit in enumerate(hits, 1):
            source = hit['_source']
            score = round(hit['_score'], 2)
            
            print(f"{i}. Drug: {source.get('drug_name', 'Unknown')} (Score: {score})")
            
            # Helper to truncate long text
            def truncate(text, length=150):
                text_str = str(text) if text else "N/A"
                if len(text_str) > length:
                    return text_str[:length] + "..."
                return text_str
            
            print(f"   Indications: {truncate(source.get('indications'))}")
            print(f"   Adverse Effects: {truncate(source.get('adverse_effects'))}")
            print(f"   Dose: {truncate(source.get('dose'))}")
            print("-" * 60)
                
    except Exception as e:
        print(f"Error during search: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "load":
            json_file = sys.argv[2] if len(sys.argv) > 2 else "CleanedFinalJson.json"
            load_data(json_file)
        elif command == "search":
            query = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else ""
            if query:
                search_database(query)
            else:
                print("Error: Search command requires a query string.")
        else:
            print("Invalid command. Use 'load' or 'search'.")
    else:
        print("Usage instructions:")
        print("-------------------")
        print("1. To load data into Elasticsearch:")
        print("   python insert_and_query.py load [filename.json]")
        print("   (filename.json is optional, defaults to CleanedFinalJson.json)")
        print("\n2. To search the database:")
        print("   python insert_and_query.py search [your query text]")
        print("   (Example: python insert_and_query.py search headache and fever)")
