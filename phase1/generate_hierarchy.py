import json
import os

def generate_hierarchy():
    base_path = r'c:\Kaif\Pepagora'
    categories_file = os.path.join(base_path, 'pepagoraDb.categories.json')
    subcategories_file = os.path.join(base_path, 'pepagoraDb.subcategories.json')
    productcategories_file = os.path.join(base_path, 'pepagoraDb.productcategories.json')
    output_file = os.path.join(base_path, 'hierarchy.json')

    print("Loading data...")
    with open(categories_file, 'r', encoding='utf-8') as f:
        categories_data = json.load(f)
    with open(subcategories_file, 'r', encoding='utf-8') as f:
        subcategories_data = json.load(f)
    with open(productcategories_file, 'r', encoding='utf-8') as f:
        productcategories_data = json.load(f)

    # Map Categories for quick lookup
    # Structure: { id: name }
    categories_map = {}
    for cat in categories_data:
        cat_id = cat['_id']['$oid']
        categories_map[cat_id] = cat['name']

    # Map Subcategories for quick lookup
    # Structure: { id: { name, parentId } }
    subcategories_map = {}
    for sub in subcategories_data:
        sub_id = sub['_id']['$oid']
        # Handle string parentId in subcategories
        parent_id = sub.get('parentId')
        subcategories_map[sub_id] = {
            'name': sub['name'],
            'parentId': parent_id
        }

    print("Processing product categories...")
    hierarchy = []
    for prod in productcategories_data:
        prod_id = prod['_id']['$oid']
        prod_name = prod['name']
        
        # parentId in productcategories is an $oid object
        sub_id_obj = prod.get('parentId')
        if not sub_id_obj:
            continue
            
        sub_id = sub_id_obj.get('$oid')
        if not sub_id or sub_id not in subcategories_map:
            continue
            
        sub = subcategories_map[sub_id]
        cat_id = sub.get('parentId')
        
        if not cat_id or cat_id not in categories_map:
            continue
            
        cat_name = categories_map[cat_id]
        
        hierarchy.append({
            "CategoryId": cat_id,
            "CategoryName": cat_name,
            "SubcategoryId": sub_id,
            "SubcategoryName": sub['name'],
            "ProductCategoryId": prod_id,
            "ProductCategoryName": prod_name
        })

    print(f"Total paths found: {len(hierarchy)}")
    print(f"Saving to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(hierarchy, f, indent=2)
    print("Done!")

if __name__ == "__main__":
    generate_hierarchy()
