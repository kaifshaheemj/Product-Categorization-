import json
import pandas as pd

# Paths
GROUND_TRUTH_FILE = r'c:\Kaif\Pepagora\dataset\pepagoraDb.liveproducts_mapped_50K.json'
# GENERATED_FILE = r'c:\Kaif\Pepagora\mappings\mapped_products_with_accuracy.xlsx'
GENERATED_FILE = r'C:\Kaif\Pepagora\phase1\bert_mapped_products.csv'

def validate():
    print(f"Loading ground truth from {GROUND_TRUTH_FILE}...")
    with open(GROUND_TRUTH_FILE, 'r', encoding='utf-8') as f:
        ground_truth_list = json.load(f)
    
    # Create a lookup map: { productName: { cat, sub, prodCat } }
    gt_map = {}
    for item in ground_truth_list:
        name = item.get('productName', '').strip()
        gt_map[name] = {
            'Main Category': item.get('category', {}).get('name', 'N/A'),
            'Sub Category': item.get('subCategory', {}).get('name', 'N/A'),
            'Product Category': item.get('productCategory', {}).get('name', 'N/A')
        }
    
    print(f"Loading generated results from {GENERATED_FILE}...")
    # Handle both CSV and Excel formats
    if GENERATED_FILE.endswith('.csv'):
        df_gen = pd.read_csv(GENERATED_FILE)
    else:
        df_gen = pd.read_excel(GENERATED_FILE)
    
    matches = {
        'Main Category': 0,
        'Sub Category': 0,
        'Product Category': 0,
        'Exact Chain': 0
    }
    total_found = 0
    
    print("\nStarting comparison...")
    for index, row in df_gen.iterrows():
        name = str(row['Product Name']).strip()
        if name in gt_map:
            total_found += 1
            gt = gt_map[name]
            
            mc_match = row['Main Category'] == gt['Main Category']
            sc_match = row['Sub Category'] == gt['Sub Category']
            pc_match = row['Product Category'] == gt['Product Category']
            
            if mc_match: matches['Main Category'] += 1
            if sc_match: matches['Sub Category'] += 1
            if pc_match: matches['Product Category'] += 1
            if mc_match and sc_match and pc_match: matches['Exact Chain'] += 1
    
    if total_found == 0:
        print("No matching products found between the files!")
        return

    print(f"\nValidation Results (based on {total_found} matched products):")
    print("-" * 50)
    for key, count in matches.items():
        percent = (count / total_found) * 100
        print(f"{key:18}: {count}/{total_found} ({percent:.2f}%)")
    print("-" * 50)

if __name__ == "__main__":
    validate()
