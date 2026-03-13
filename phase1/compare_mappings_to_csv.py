import json
import pandas as pd
import os

# Paths
GROUND_TRUTH_FILE = r'c:\\Kaif\\Pepagora\\dataset\\pepagoraDb.liveproducts_mapped_50K.json'
GENERATED_FILE = r'c:\\Kaif\\Pepagora\\mappings\\mapped_products_with_accuracy.xlsx'
COMPARISON_FILE = r'c:\\Kaif\\Pepagora\\mappings\\comparison2.csv'

def compare_to_csv():
    print(f"Loading ground truth from {GROUND_TRUTH_FILE}...")
    with open(GROUND_TRUTH_FILE, 'r', encoding='utf-8') as f:
        ground_truth_list = json.load(f)
    
    # Create a list for ground truth data
    gt_data = []
    for item in ground_truth_list:
        gt_data.append({
            "Product Name": item.get('productName', '').strip(),
            "Product Description": item.get('productDescription', '').strip(),
            "Actual Main Category": item.get('category', {}).get('name', 'N/A'),
            "Actual Sub Category": item.get('subCategory', {}).get('name', 'N/A'),
            "Actual Product Category": item.get('productCategory', {}).get('name', 'N/A')
        })
    df_gt = pd.DataFrame(gt_data)
    
    print(f"Loading generated results from {GENERATED_FILE}...")
    # Check extension to use correct pandas reader
    if GENERATED_FILE.endswith('.xlsx'):
        df_gen = pd.read_excel(GENERATED_FILE)
    else:
        df_gen = pd.read_csv(GENERATED_FILE)
    
    # Rename generated columns for clarity
    df_gen = df_gen.rename(columns={
        "Main Category": "Generated Main Category",
        "Sub Category": "Generated Sub Category",
        "Product Category": "Generated Product Category",
        "Accuracy": "Generated Accuracy"
    })

    # Merge dataframes on Product Name
    print("Merging data for side-by-side comparison...")
    # Using 'inner' so we only see the ones we generated (the 50 samples)
    # Select only the ground truth category columns to avoid merge conflicts with Description
    df_gt_subset = df_gt[["Product Name", "Actual Main Category", "Actual Sub Category", "Actual Product Category"]]
    df_combined = pd.merge(df_gen, df_gt_subset, on="Product Name", how="inner")

    # Reorder columns for better readability
    cols = [
        "Product Name", 
        "Product Description",
        "Actual Main Category", "Generated Main Category",
        "Actual Sub Category", "Generated Sub Category",
        "Actual Product Category", "Generated Product Category",
        "Generated Accuracy"
    ]
    df_combined = df_combined[cols]

    print(f"Saving comparison to {COMPARISON_FILE}...")
    df_combined.to_csv(COMPARISON_FILE, index=False)
    print("Done! Comparison file created.")

if __name__ == "__main__":
    compare_to_csv()
