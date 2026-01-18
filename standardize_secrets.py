import os

TARGET_STRING = "azure-storage-connection-string"
REPLACEMENT_STRING = "az-cs"

def standardize_yaml_file(filepath):
    print(f"Processing: {filepath}")
    with open(filepath, 'r') as f:
        content = f.read()
    
    if TARGET_STRING not in content:
        print(f"  - No occurrence of '{TARGET_STRING}' found.")
        return

    new_content = content.replace(TARGET_STRING, REPLACEMENT_STRING)
    
    with open(filepath, 'w') as f:
        f.write(new_content)
    print(f"  + Replaced occurrences.")

def main():
    deploy_dir = "deploy"
    for filename in os.listdir(deploy_dir):
        if filename.endswith(".yaml") or filename.endswith(".yml"):
            standardize_yaml_file(os.path.join(deploy_dir, filename))

if __name__ == "__main__":
    main()
