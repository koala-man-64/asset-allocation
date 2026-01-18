import os
import yaml

# Required Environment Variables block to inject/verify
REQUIRED_ENVS = {
    "AZURE_CONTAINER_MARKET": "gold",
    "AZURE_CONTAINER_FINANCE": "gold",
    "AZURE_CONTAINER_EARNINGS": "gold",
    "AZURE_CONTAINER_TARGETS": "gold",
    "AZURE_CONTAINER_COMMON": "common",
    "AZURE_CONTAINER_RANKING": "ranking-data",
    "AZURE_CONTAINER_BRONZE": "bronze",
    "AZURE_CONTAINER_SILVER": "silver",
    "LOG_FORMAT": "JSON"
}

def update_yaml_file(filepath):
    print(f"Processing: {filepath}")
    with open(filepath, 'r') as f:
        # Load as simple lines to preserve comments/structure better than full parse/dump 
        # But for reliability in "env" list, we'll use a pragmatic line-based approach 
        # or a safe yaml loader. 
        # Given we want to preserve formatting, let's look for the "env:" block.
        lines = f.readlines()

    new_lines = []
    in_env_block = False
    env_indent = ""
    existing_vars = set()
    
    # First pass: identify existing vars
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- name:"):
            var_name = stripped.split("name:")[1].strip()
            existing_vars.add(var_name)

    # We need to find where to insert. 
    # Usually under "containers:\n    - env:"
    
    # Let's use a simpler insert strategy: Find the start of "env:" and append to it?
    # Or keep it simple: Read file, find lines with "- name: X", track them.
    # At the end of the "env" block, inject missing ones.
    
    # Actually, let's just use PyYAML but be careful. 
    # PyYAML usually ruins formatting. 
    # Let's try string manipulation.
    
    env_start_index = -1
    container_indent = ""
    
    for i, line in enumerate(lines):
        if line.strip() == "env:" or line.strip() == "- env:":
            env_start_index = i
            # count spaces
            container_indent = line.split(line.strip())[0]
            break
            
    if env_start_index == -1:
        print(f"Skipping {filepath} (no env block found)")
        return

    # Scan forward to find end of env block (next line with same indent as 'env:' or less)
    # Actually, env items are indented further.
    
    insertion_point = env_start_index + 1
    
    # We'll just insert right after "env:"
    # Indentation should be container_indent + "  " (2 spaces) or similar.
    # Check next line to guess indent
    
    item_indent = container_indent + "  " # Default guess
    if len(lines) > env_start_index + 1:
        next_line = lines[env_start_index + 1]
        if next_line.strip().startswith("- name:"):
            item_indent = next_line.split("- name:")[0]
            
    # Prepare insertions
    insertions = []
    for key, default_val in REQUIRED_ENVS.items():
        if key not in existing_vars:
            insertions.append(f"{item_indent}- name: {key}\n")
            insertions.append(f"{item_indent}  value: {default_val}\n")
            print(f"  + Adding {key}")
            
    # Reconstruct file
    final_lines = lines[:insertion_point] + insertions + lines[insertion_point:]
    
    with open(filepath, 'w') as f:
        f.writelines(final_lines)

def main():
    deploy_dir = "deploy"
    for filename in os.listdir(deploy_dir):
        if filename.endswith(".yaml") or filename.endswith(".yml"):
            update_yaml_file(os.path.join(deploy_dir, filename))

if __name__ == "__main__":
    main()
