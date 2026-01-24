import os
import re

def refactor_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Mappings for imports and patch strings
    replacements = [
        # scripts.common -> asset_allocation.core
        (r'scripts\.common', 'asset_allocation.core'),
        
        # scripts.<module> -> asset_allocation.tasks.<module>
        (r'scripts\.ranking', 'asset_allocation.tasks.ranking'),
        (r'scripts\.earnings_data', 'asset_allocation.tasks.earnings_data'),
        (r'scripts\.finance_data', 'asset_allocation.tasks.finance_data'),
        (r'scripts\.market_data', 'asset_allocation.tasks.market_data'),
        (r'scripts\.monitoring', 'asset_allocation.tasks.monitoring'),
        (r'scripts\.price_target_data', 'asset_allocation.tasks.price_target_data'),
        
        # api -> asset_allocation.api
        (r'from api(?!\.|_)', 'from asset_allocation.api'),
        (r'import api(?!\.|_)', 'import asset_allocation.api'),
        (r'api\.main', 'asset_allocation.api.main'),
        (r'api\.endpoints', 'asset_allocation.api.endpoints'),
        (r'"api\.', '"asset_allocation.api.'),
        (r"'api\.", "'asset_allocation.api."),
        
        # alpaca -> asset_allocation.alpaca
        (r'from alpaca', 'from asset_allocation.alpaca'),
        (r'import alpaca', 'import asset_allocation.alpaca'),
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
    test_dir = r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation\tests"
    count = 0
    for subdir, dirs, files in os.walk(test_dir):
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(subdir, file)
                if refactor_file(filepath):
                    print(f"Refactored: {filepath}")
                    count += 1
    print(f"Total test files refactored: {count}")

if __name__ == "__main__":
    main()
