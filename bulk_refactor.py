import os
import re

def refactor_file(filepath, log_file):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    replacements = [
        (r'scripts\.common', 'asset_allocation.core'),
        (r'scripts\.ranking', 'asset_allocation.tasks.ranking'),
        (r'scripts\.earnings_data', 'asset_allocation.tasks.earnings_data'),
        (r'scripts\.finance_data', 'asset_allocation.tasks.finance_data'),
        (r'scripts\.market_data', 'asset_allocation.tasks.market_data'),
        (r'scripts\.monitoring', 'asset_allocation.tasks.monitoring'),
        (r'scripts\.price_target_data', 'asset_allocation.tasks.price_target_data'),
        (r'api(?!\.|_)', 'asset_allocation.api'),
        (r'api\.', 'asset_allocation.api.'),
        (r'alpaca(?!\.|_)', 'asset_allocation.alpaca'),
        (r'alpaca\.', 'asset_allocation.alpaca.'),
    ]

    new_content = content
    changes = []
    for pattern, replacement in replacements:
        if re.search(pattern, new_content):
            new_content = re.sub(pattern, replacement, new_content)
            changes.append(f"{pattern} -> {replacement}")

    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        log_file.write(f"UPDATED: {filepath} | {', '.join(changes)}\n")
        return True
    return False

def main():
    base_dir = r"c:\Users\rdpro\Projects\AssetAllocation - AG\asset-allocation\src\asset_allocation"
    with open("refactor_log.txt", "w", encoding='utf-8') as log_file:
        count = 0
        for subdir, dirs, files in os.walk(base_dir):
            for file in files:
                if file.endswith(".py"):
                    filepath = os.path.join(subdir, file)
                    if refactor_file(filepath, log_file):
                        count += 1
        log_file.write(f"\nTotal files refactored: {count}\n")
    print(f"Done. Refactored {count} files. Check refactor_log.txt")

if __name__ == "__main__":
    main()
