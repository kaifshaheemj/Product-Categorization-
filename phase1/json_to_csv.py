import json
import pandas as pd
import os

# --- CONFIGURATION ---
INPUT_JSON = r'c:\Kaif\Pepagora\dataset\pepagoraDb.liveproducts_mapped_50K.json'
OUTPUT_CSV = r'c:\Kaif\Pepagora\dataset\pepagoraDb.liveproducts_mapped_50K.csv'

def convert_json_to_csv():
    print(f"Loading JSON data from {INPUT_JSON}...")
    with open(INPUT_JSON, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print("Flattening nested structure...")
    flattened_data = []
    for item in data:
        row = {
            "ID": item.get('_id', {}).get('$oid', ''),
            "Product Name": item.get('productName', ''),
            "Product Description": item.get('productDescription', ''),
            "Actual Main Category": item.get('category', {}).get('name', ''),
            "Actual Sub Category": item.get('subCategory', {}).get('name', ''),
            "Actual Product Category": item.get('productCategory', {}).get('name', '')
        }
        flattened_data.append(row)
    
    print(f"Creating DataFrame and saving to {OUTPUT_CSV}...")
    df = pd.DataFrame(flattened_data)
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig') # Using sig to handle special characters in Excel
    
    print(f"Successfully converted {len(df)} records to CSV.")

if __name__ == "__main__":
    try:
        convert_json_to_csv()
    except Exception as e:
        print(f"An error occurred: {e}")
