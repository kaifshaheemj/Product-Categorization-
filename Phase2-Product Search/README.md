# Pepagora Phase 2: Product Search & Business Profiling

An advanced product search system built on Elasticsearch featuring business profile cards, multi-tier ranking, and contextual semantic retrieval.

## 🚀 Key Features

- **Advanced Ranking**: Businesses are ranked by tier: `Global` > `Scale` > `Grow` > `Free`. 
- **Business Profile Cards**: Rich UI cards showing logo, tier badge, country, and product relevance.
- **Dual-Section Autocomplete**:
  - **Search Suggestions**: Instantly matched product names.
  - **Matching Businesses**: Full business cards directly in the dropdown.
- **Search Strategy**: Multi-layered search combining:
  - Phrase matching (product & business names)
  - Cross-fields token search
  - Contextual retrieval (description-based matching)
  - Fuzzy matching (typo tolerance)
- **Interactive UI**: Premium red & white theme with sidebar filters (Tier, Category, Country) and live stats.

## 📂 Project Structure

- `products_index_v2.py`: Elasticsearch index configuration (mappings, analyzers, synonyms).
- `bulk_index_v2.py`: Data ingestion script (joins products with business logos/tiers).
- `search_api_v2.py`: FastAPI backend with `/search`, `/autocomplete`, and `/suggest` endpoints.
- `search_ui_v2.html`: Premium frontend interface.

## 🛠️ Setup & Running

### 1. Requirements
Ensure you have the `elasticsearch` and `fastapi` libraries installed in your environment:
```powershell
pip install elasticsearch fastapi uvicorn ijson
```

### 2. Step-by-Step Execution

#### Step A: Create the Index
Creates the `products_v2` index with custom analyzers.
```powershell
python products_index_v2.py
```

#### Step B: Index the Data
Joins `preprocessed_products.json` with business profile details and streams them to Elasticsearch.
```powershell
python bulk_index_v2.py
```

#### Step C: Start the Search API
Runs the FastAPI server on port 8001.
```powershell
python -m uvicorn search_api_v2:app --reload --port 8001
```

#### Step D: Access the UI
Open your browser and navigate to:
**[http://127.0.0.1:8001](http://127.0.0.1:8001)**

## 🔍 API Endpoints

- `GET /search`: Main search with filtering and boosting.
- `GET /autocomplete`: Deduplicated business cards for the dropdown.
- `GET /suggest`: Raw product name suggestions for the dropdown.
- `GET /health`: System connection and doc count status.
