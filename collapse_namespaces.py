import os
import re

def collapse_namespace(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        return False

    # Collapse asset_allocation.asset_allocation[.asset_allocation...]
    pattern = r'asset_allocation\.(asset_allocation\.)+asset_allocation'
    new_content = re.sub(pattern, 'asset_allocation', content)
    
    # Also handle intermediate ones like asset_allocation.asset_allocation.api
    pattern2 = r'asset_allocation\.(asset_allocation\.)+'
    new_content = re.sub(pattern2, 'asset_allocation.', new_content)

    if new_content != content:
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
                    if collapse_namespace(os.path.join(subdir, file)):
                        count += 1
    print(f"Collapsed namespaces in {count} files.")

if __name__ == "__main__":
    main()
