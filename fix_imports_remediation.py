import os
import re

def refactor_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Mappings
    replacements = [
        # Common / Core
        (r'(from|import) scripts\.common', r'\1 asset_allocation.core'),
        
        # Sub-tasks / Scripts
        (r'(from|import) scripts\.ranking', r'\1 asset_allocation.tasks.ranking'),
        (r'(from|import) scripts\.earnings_data', r'\1 asset_allocation.tasks.earnings_data'),
        (r'(from|import) scripts\.finance_data', r'\1 asset_allocation.tasks.finance_data'),
        (r'(from|import) scripts\.market_data', r'\1 asset_allocation.tasks.market_data'),
        (r'(from|import) scripts\.monitoring', r'\1 asset_allocation.tasks.monitoring'),
        (r'(from|import) scripts\.price_target_data', r'\1 asset_allocation.tasks.price_target_data'),
        (r'(from|import) scripts\.ranking', r'\1 asset_allocation.tasks.ranking'),
        
        # API / Alpaca
        (r'(from|import) api(?!\.|_)', r'\1 asset_allocation.api'),
        (r'(from|import) api\.', r'\1 asset_allocation.api.'),
        (r'(from|import) alpaca(?!\.|_)', r'\1 asset_allocation.alpaca'),
        (r'(from|import) alpaca\.', r'\1 asset_allocation.alpaca.'),
    ]

    new_content = content
    for pattern, replacement in replacements:
        new_content = re.sub(pattern, replacement, new_content)

    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        return True
    return False

def main():
    base_dir = r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation\src\asset_allocation"
    count = 0
    for subdir, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(subdir, file)
                if refactor_file(filepath):
                    print(f"Refactored: {filepath}")
                    count += 1
    print(f"Total files refactored: {count}")

if __name__ == "__main__":
    main()
