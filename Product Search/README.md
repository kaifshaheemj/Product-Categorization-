# Professional Product Search System

A high-performance, e-commerce style product search system featuring real-time autocomplete, context-aware indexing, and a premium "Red & White" UI theme.

## 🚀 Quick Start

1. **Environment Setup**
   Ensure you are using the provided virtual environment:
   ```powershell
   # Activate Venv
   C:\DataGenie\dg_venv\Scripts\activate
   ```

2. **Elasticsearch Connection**
   The system is configured to connect to:
   - **Host**: `https://127.0.0.1:9201`
   - **Username**: `elastic`
   - **Password**: `dcJwpNq4m6wtDNpdbUhY`
   *(SSL verification is disabled for development)*

3. **Initialize the Index**
   Recreate the index with optimized e-commerce mappings (synonyms, stemmers, edge n-grams):
   ```powershell
   python products_index.py
   ```

4. **Load the Data**
   Index the 100,000 product records from the CSV:
   ```powershell
   python bulk_index.py
   ```

5. **Run the Search UI**
   Start the FastAPI backend (serves the frontend UI on port 8000):
   ```powershell
   python -m uvicorn autocomplete_api:app --reload --port 8000
   ```
   Visit: **[http://localhost:8000/](http://localhost:8000/)**

---

## 🏗️ Architecture

### 1. Indexing & Analysis (`products_index.py`)
- **Analyzers**: Uses `standard` tokens with `lowercase`, `english_stop`, and `english_stemmer`.
- **Autocomplete**: Uses `edge_ngram` (min 2, max 20) on the `productName.autocomplete` field for instant character-by-character matching.
- **Synonyms**: Pre-configured synonyms for common industrial and consumer terms (e.g., "cap" → "capacitor", "ups" → "uninterruptible power supply").

### 2. Backend API (`autocomplete_api.py`)
- **Framework**: FastAPI.
- **Endpoints**:
  - `GET /autocomplete?q=...`: Light-weight suggestion endpoint (returns 6 matches).
  - `GET /search?q=...&page=N`: Full results with pagination and category aggregations.
- **Static Hosting**: Serves `autocomplete.html` as the root index page.

### 3. Frontend UI (`autocomplete.html`)
- **Theme**: Premium "Red & White" (Apple/Tesla style).
- **Features**: 
  - Dynamic dropdown with category icons.
  - Keyboard navigation (Arrow keys + Enter).
  - Category sidebar filtering.
  - Responsive grid layout (Desktop: 2-column, Mobile: 1-column).
  - **Auto-scroll fix**: The dropdown is bounded with a scrollbar to ensure visibility on all screen heights.

---

## 🛠️ File Reference

| File | Purpose |
| :--- | :--- |
| `products_index.py` | Defines Elasticsearch mapping and creates the index. |
| `bulk_index.py` | Efficiently pushes 100k CSV rows to ES using `helpers.parallel_bulk`. |
| `autocomplete_api.py` | The main server providing search logic and UI serving. |
| `autocomplete.html` | The HTML/CSS/JS frontend application. |
| `smart_search.py` | Advanced search logic utility for testing complex queries. |
| `csv_to_json.py` | Utility to convert raw CSV data to JSON for better formatting. |

---

## 💡 Key Design Decisions
- **Stacking Context**: The `.hero` section is given `z-index: 10` and `overflow: visible` to ensure the autocomplete dropdown floats over the entire page content.
- **Performance**: Autocomplete is debounced by 220ms on the frontend to minimize unnecessary API hits during fast typing.
- **UX**: Search result counts are aggregated in real-time in the sidebar allowing users to filter by 15+ industrial categories instantly.
