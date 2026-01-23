
import os
import re

def refactor_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Pattern 1: from xyz import ... -> from xyz import ...
    # This covers: from backtest import Engine
    # Becomes: from backtest import Engine
    content = re.sub(r'from asset_allocation\.', 'from ', content)
    
    # Pattern 2: import xyz -> import xyz
    content = re.sub(r'import asset_allocation\.', 'import ', content)
    
    # Pattern 3: Usage in code, e.g. x = backtest.Engine()
    # Ideally imports are fixed, this handles fully qualified names IF they were used.
    # We should be careful. replacing "" globally is strictly what flattens it.
    # But let's check for "my_asset_allocation" variable names. 
    # The regex dot matches literal dot.
    # We use lookbehind to ensure we don't match "my_asset_allocation."
    
    # " " -> " "
    # "(" -> "("
    # etc.
    # Safest is to replace "" with "" but only where it looks like a package prefix.
    # Actually, simplest approach that covers 99% cases for this specific refactor is:
    content = re.sub(r'(?<![a-zA-Z0-9_])asset_allocation\.', '', content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

def main():
    root_dir = os.getcwd()
    for subdir, dirs, files in os.walk(root_dir):
        if ".git" in subdir or "node_modules" in subdir or "ui" in subdir:
            continue
            
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(subdir, file)
                print(f"Processing {filepath}...")
                refactor_file(filepath)

if __name__ == "__main__":
    main()
