import os
import ast
import sys
# import stdlib_list

# Mapping from import name to package name in requirements.txt
# This list handles common discrepancies.
IMPORT_TO_PACKAGE = {
    "bs4": "beautifulsoup4",
    "dotenv": "python-dotenv",
    "dateutil": "python-dateutil",
    "yaml": "PyYAML",
    "cv2": "opencv-python",
    "sklearn": "scikit-learn",
    "PIL": "Pillow",
    "psycopg": "psycopg",  # or psycopg-binary
    "typing_extensions": "typing-extensions",
}

# Add standard library modules for Python 3.10+
# sys.stdlib_module_names is available in Python 3.10+
try:
    STD_LIB = sys.stdlib_module_names
except AttributeError:
    # Fallback or just use a basic set. 
    # For this environment, we can assume a recent python or use a library if available.
    # We will try to rely on is_std_lib checks if possible, or just build a large set.
    STD_LIB = set(sys.builtin_module_names)

def is_std_lib(name):
    if name in STD_LIB:
        return True
    return False

def get_imports(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            tree = ast.parse(f.read(), filename=file_path)
        except SyntaxError:
            return set()
    
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split('.')[0])
    return imports

def main():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root_dir)
    all_imports = set()
    
    packages_in_requirements = set()
    try:
        with open("requirements.txt", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    # Remove version specifiers
                    pkg = line.split("==")[0].split(">=")[0].split("<=")[0].split("~=")[0].split("[")[0]
                    packages_in_requirements.add(pkg.lower())
    except FileNotFoundError:
        print("requirements.txt not found.")
        return

    # print(f"Packages in requirements: {packages_in_requirements}")

    # Add common aliases to the requirements check set
    # We want to check if the IMPORT name is covered.
    # So if "pandas" is imported, we check if "pandas" is in requirements.
    # If "bs4" is imported, we check if "beautifulsoup4" is in requirements.
    
    # Let's verify files
    for subdir, dirs, files in os.walk(root_dir):
        if ".venv" in subdir or "__pycache__" in subdir or ".git" in subdir:
            continue
        
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(subdir, file)
                imps = get_imports(path)
                all_imports.update(imps)

    # Filter imports
    missing_packages = set()
    
    # We need to know which imports are LOCAL modules vs 3rd party.
    # We can detect local modules by checking if a folder/file exists.
    # But for now let's just collect all and check against requirements + stdlib.
    
    local_modules = set()
    for item in os.listdir(root_dir):
        if os.path.isdir(item):
            local_modules.add(item)
        elif item.endswith(".py"):
            local_modules.add(item[:-3])

    # Also check sub-packages (directories with __init__.py) - simplified check above.

    for imp in all_imports:
        if imp.startswith("_"): continue # Internal/C modules often
        if imp in STD_LIB: continue
        if imp in local_modules: continue
        
        # Check against requirements
        req_name = IMPORT_TO_PACKAGE.get(imp, imp)
        
        # Normalize for comparison (pkg_resources convention is usually dash replacement?)
        # requirements usually use hyphens, imports use underscores (e.g. azure_storage_blob vs azure-storage-blob is tricky)
        # Actually imports are usually packages. 
        # "import azure.storage.blob" -> "azure" is the top level.
        # But "azure-storage-blob" is the package.
        # "azure" namespace packages are special.
        
        # Special handling for azure
        if imp == "azure":
            # We can't easily validate this without looking at sub-imports.
            # But requirements has "azure-storage-blob" etc.
            # Let's rely on manual review for azure if needed, or assume it's covered if any azure-* pkg exists.
            has_azure = any(r.startswith("azure-") for r in packages_in_requirements)
            if has_azure: continue

        if req_name.lower() not in packages_in_requirements and req_name.replace("-", "_").lower() not in packages_in_requirements:
             missing_packages.add(imp)

    print("--- Analysis Result ---")
    if missing_packages:
        print("Potentially missing packages (imports found but not in requirements.txt):")
        for pkg in sorted(missing_packages):
            print(f"  {pkg}")
    else:
        print("No missing packages detected based on static analysis.")

if __name__ == "__main__":
    main()
