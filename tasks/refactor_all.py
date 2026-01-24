
import os
import re

TARGET_DIRS = ['alpaca', 'api', 'backtest', 'monitoring', 'scripts', 'tests']

def refactor_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Pattern 1: from xyz import ... -> from xyz import ...
    content = re.sub(r'from asset_allocation\.', 'from ', content)
    
    # Pattern 2: import xyz -> import xyz
    content = re.sub(r'import asset_allocation\.', 'import ', content)
    
    # Pattern 3: Usage in code
    content = re.sub(r'(?<![a-zA-Z0-9_])asset_allocation\.', '', content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

def main():
    root_dir = os.getcwd()
    
    for target in TARGET_DIRS:
        target_path = os.path.join(root_dir, target)
        if not os.path.exists(target_path):
            print(f"Skipping {target_path} (not found)")
            continue
            
        print(f"Scanning {target_path}...")
        for subdir, dirs, files in os.walk(target_path):
            if "__pycache__" in subdir:
                continue
                
            for file in files:
                if file.endswith(".py"):
                    filepath = os.path.join(subdir, file)
                    # print(f"Processing {filepath}...")
                    refactor_file(filepath)
    print("Done.")

if __name__ == "__main__":
    main()
