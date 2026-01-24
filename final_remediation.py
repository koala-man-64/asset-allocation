import os
import re

REPLACEMENTS = [
    # Common Core
    (r'scripts\.common', 'asset_allocation.core'),
    
    # Task specific
    (r'scripts\.ranking', 'asset_allocation.tasks.ranking'),
    (r'scripts\.earnings_data', 'asset_allocation.tasks.earnings_data'),
    (r'scripts\.finance_data', 'asset_allocation.tasks.finance_data'),
    (r'scripts\.market_data', 'asset_allocation.tasks.market_data'),
    (r'scripts\.monitoring', 'asset_allocation.tasks.monitoring'),
    (r'scripts\.price_target_data', 'asset_allocation.tasks.price_target_data'),
    
    # API / Alpaca
    (r'from api(?!\.|_)', 'from asset_allocation.api'),
    (r'import api(?!\.|_)', 'import asset_allocation.api'),
    (r'api\.main', 'asset_allocation.api.main'),
    (r'api\.endpoints', 'asset_allocation.api.endpoints'),
    (r'"api\.', '"asset_allocation.api.'),
    (r"'api\.", "'asset_allocation.api."),
    
    (r'from alpaca', 'from asset_allocation.alpaca'),
    (r'import alpaca', 'import asset_allocation.alpaca'),
]

def refactor_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        return False

    new_content = content
    changed = False
    for pattern, replacement in REPLACEMENTS:
        if re.search(pattern, new_content):
            new_content = re.sub(pattern, replacement, new_content)
            changed = True

    if changed:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        return True
    return False

def main():
    roots = [
        r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation\src\asset_allocation",
        r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation\tests",
        r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation\backtest",
        r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation\monitoring"
    ]
    count = 0
    for root in roots:
        for subdir, dirs, files in os.walk(root):
            for file in files:
                if file.endswith(".py"):
                    filepath = os.path.join(subdir, file)
                    if refactor_file(filepath):
                        print(f"Refactored: {filepath}")
                        count += 1
    print(f"Done. Refactored {count} files.")

if __name__ == "__main__":
    main()
