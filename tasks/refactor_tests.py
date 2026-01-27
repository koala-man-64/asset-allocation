
import os
import re

def refactor_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Pattern 1: from xyz import ... -> from xyz import ...
    content = re.sub(r'from asset_allocation\.', 'from ', content)
    
    # Pattern 2: import xyz -> import xyz
    content = re.sub(r'import asset_allocation\.', 'import ', content)
    
    # Pattern 3: Usage in code, e.g. x = engine.Engine()
    content = re.sub(r'(?<![a-zA-Z0-9_])asset_allocation\.', '', content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

def main():
    root_dir = os.path.join(os.getcwd(), 'tests')
    print(f"Scanning {root_dir}...")
    
    for subdir, dirs, files in os.walk(root_dir):
        if "__pycache__" in subdir:
            continue
            
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(subdir, file)
                print(f"Processing {filepath}...")
                refactor_file(filepath)

if __name__ == "__main__":
    main()
