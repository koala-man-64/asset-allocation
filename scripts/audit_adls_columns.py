
import sys
import os
import argparse
import pandas as pd
from azure.storage.blob import BlobServiceClient
from deltalake import DeltaTable

# Add project root to path
sys.path.append(os.getcwd())
try:
    from scripts.common import config as cfg
except ImportError:
    pass

def get_service_client():
    if getattr(cfg, "AZURE_STORAGE_CONNECTION_STRING", None):
        return BlobServiceClient.from_connection_string(cfg.AZURE_STORAGE_CONNECTION_STRING), cfg.AZURE_STORAGE_ACCOUNT_NAME
    
    # Fallback
    account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "assetallocstorage001")
    from azure.identity import DefaultAzureCredential
    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url, credential=DefaultAzureCredential()), account_name

def list_root_folders(container_client):
    folders = set()
    # List one level deep approx
    for blob in container_client.list_blobs():
        if '/' in blob.name:
            root = blob.name.split('/')[0]
            folders.add(root)
    return sorted(list(folders))

def find_first_subfolder(container_client, prefix):
    # Walk blobs starting with prefix
    blobs = list(container_client.list_blobs(name_starts_with=prefix + "/"))
    if not blobs:
        return None
    # Find first subfolder component
    first = blobs[0].name
    parts = first.split('/')
    if len(parts) > 1:
        base_len = len(prefix.split('/'))
        if len(parts) > base_len:
            return "/".join(parts[:base_len+1])
    return None

def analyze_dataset(c_name, root, account_name, container_client):
    """Analyze a single dataset root to determine type and schema."""
    result = {
        "name": root,
        "type": "Unknown",
        "columns": [],
        "sample": None,
        "error": None
    }
    
    # Check 1: Root is a table
    uri = f"abfss://{c_name}@{account_name}.dfs.core.windows.net/{root}"
    try:
        dt = DeltaTable(uri, storage_options={"use_azure_cli": "true", "account_name": account_name})
        result["columns"] = dt.to_pyarrow_dataset().schema.names
        result["type"] = "Table"
        return result
    except Exception:
        pass
    
    # Check 2: Partitioned
    sample_sub = find_first_subfolder(container_client, root)
    if sample_sub:
        uri = f"abfss://{c_name}@{account_name}.dfs.core.windows.net/{sample_sub}"
        try:
            dt = DeltaTable(uri, storage_options={"use_azure_cli": "true", "account_name": account_name})
            result["columns"] = dt.to_pyarrow_dataset().schema.names
            result["type"] = "Partitioned"
            result["sample"] = sample_sub
            return result
        except Exception:
            pass
            
    result["error"] = "Could not identify Delta Table schema"
    return result

def audit_containers(client, account_name, containers):
    audit_data = {}
    
    for c_name in containers:
        container_data = []
        try:
            cc = client.get_container_client(c_name)
            if not cc.exists():
                audit_data[c_name] = {"error": "Container not found"}
                continue
                
            roots = list_root_folders(cc)
            if not roots:
                audit_data[c_name] = {"error": "Container empty"}
                continue
            
            for root in roots:
                dataset_info = analyze_dataset(c_name, root, account_name, cc)
                container_data.append(dataset_info)
            
            audit_data[c_name] = {"datasets": container_data}

        except Exception as e:
            audit_data[c_name] = {"error": str(e)}
            
    return audit_data

def print_text_report(audit_data):
    print("ADLS Data Audit Report")
    print("=" * 60)
    
    for container, data in audit_data.items():
        print(f"\nCONTAINER: {container}")
        print("-" * 30)
        
        if "error" in data:
            print(f"  [Error] {data['error']}")
            continue
            
        for ds in data.get("datasets", []):
            if ds["type"] == "Unknown":
                print(f"  DATASET: {ds['name']} (Unknown/Error)")
            else:
                extra = f", Sample: {ds['sample']}" if ds['sample'] else ""
                print(f"  DATASET: {ds['name']} ({ds['type']}{extra})")
                print(f"    Columns: {ds['columns']}")

def generate_markdown(audit_data, account_name, output_path):
    with open(output_path, 'w') as f:
        f.write("# Data Availability Audit Report\n\n")
        f.write(f"**Generated:** {pd.Timestamp.now()}\n")
        f.write(f"**Account:** `{account_name}`\n\n")
        
        for container, data in audit_data.items():
            f.write(f"## Container: `{container}`\n\n")
            
            if "error" in data:
                f.write(f"> **Error:** {data['error']}\n\n")
                continue
            
            datasets = data.get("datasets", [])
            if not datasets:
                f.write("*No datasets found.*\n\n")
                continue
            
            for ds in datasets:
                f.write(f"### Dataset: `{ds['name']}`\n")
                if ds['type'] == 'Unknown':
                    f.write(f"> *Status: Could not identify Delta Table schema*\n\n")
                else:
                    f.write(f"- **Type**: {ds['type']}")
                    if ds['sample']:
                        f.write(f" (Sample: `{ds['sample']}`)")
                    f.write("\n")
                    f.write(f"- **Columns**: {', '.join(f'`{c}`' for c in ds['columns'])}\n\n")

def main():
    parser = argparse.ArgumentParser(description="Audit ADLS Data Columns")
    parser.add_argument("--output", help="Path to save markdown report (e.g. docs/audit.md)")
    args = parser.parse_args()

    client, account_name = get_service_client()
    containers = ["silver", "gold"]
    
    print(f"Scanning containers: {containers}...")
    audit_data = audit_containers(client, account_name, containers)
    
    if args.output:
        print(f"Generating Markdown report at: {args.output}")
        generate_markdown(audit_data, account_name, args.output)
        print("Done.")
    else:
        print_text_report(audit_data)

if __name__ == "__main__":
    main()
