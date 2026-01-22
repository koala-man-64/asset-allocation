import sys
import os
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from typing import List, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from scripts.common import delta_core
    # Config removed to avoid strict env checks in standalone mode
except ImportError:
    print("Error: Could not import project scripts. Install the project (e.g. `pip install -e .`).")
    print("Ensure you actiavted the environment where 'deltalake' and 'azure-identity' are installed.")
    sys.exit(1)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_service_client() -> BlobServiceClient:
    """Initialize BlobServiceClient using environment credentials."""
    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)
    
    account_name = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
    if account_name:
        from azure.identity import DefaultAzureCredential
        account_url = f"https://{account_name}.blob.core.windows.net"
        # Increase timeout for listing many containers? Default is usually fine.
        return BlobServiceClient(account_url, credential=DefaultAzureCredential())
    
    raise ValueError("No valid Azure Storage credentials found (set AZURE_STORAGE_ACCOUNT_NAME or CONNECTION_STRING).")

def select_container(client: BlobServiceClient) -> Optional[str]:
    """List containers and prompt user to select one."""
    while True:
        print("\n--- Select Container ---")
        try:
            containers = list(client.list_containers())
        except Exception as e:
            print(f"Error listing containers: {e}")
            return None

        for i, c in enumerate(containers):
            print(f"[{i+1}] {c.name}")
        print("[q] Quit")

        choice = input("\nSelect container index: ").strip().lower()
        if choice == 'q':
            return None
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(containers):
                return containers[idx].name
            else:
                print("Invalid index.")
        except ValueError:
            print("Invalid input.")

def inspect_delta_table(container: str, path: str, rows: int = 5):
    """Load and print Delta Table details."""
    print(f"\nLOADING DELTA TABLE: {container}/{path} ...")
    try:
        df = delta_core.load_delta(container, path)
        if df is None:
            print("Result: None (Table not found or empty)")
        else:
            print(f"Shape: {df.shape}")
            print(f"Columns: {df.columns.tolist()}")
            print(f"\nHead({rows}):")
            print(df.head(rows).to_string())
            
            if 'date' in df.columns:
                 print("\nUnique dates:", sorted(df['date'].unique()))
    except Exception as e:
        print(f"Error loading Delta Table: {e}")
    input("\nPress Enter to continue...")

def inspect_file(container_client, path: str):
    """Download and inspect a single file."""
    print(f"\nINSPECTING FILE: {path}")
    try:
        blob = container_client.get_blob_client(path)
        props = blob.get_blob_properties()
        print(f"Size: {props.size} bytes")
        print(f"Last Modified: {props.last_modified}")
        
        if path.endswith('.csv'):
            print("Downloading CSV preview...")
            data = blob.download_blob().readall()
            df = pd.read_csv(io.BytesIO(data))
            print(df.head().to_string())
        elif path.endswith('.parquet'):
            print("Downloading Parquet preview...")
            data = blob.download_blob().readall()
            df = pd.read_parquet(io.BytesIO(data))
            print(df.head().to_string())
        elif path.endswith('.txt') or path.endswith('.json') or path.endswith('.md'):
            print("Content preview:")
            data = blob.download_blob().readall()
            print(data.decode('utf-8')[:1000])
            
    except Exception as e:
        print(f"Error inspecting file: {e}")
    input("\nPress Enter to continue...")

def navigate_container(client: BlobServiceClient, container_name: str):
    """Navigation loop for a specific container."""
    container_client = client.get_container_client(container_name)
    current_prefix = ""

    while True:
        clear_screen()
        print(f"--- BROWSER: {container_name}/{current_prefix} ---")
        
        # List items (simulated directory walk)
        # walk_blobs(name_starts_with=prefix, delimiter='/') returns ItemPaged
        # Items can be BlobProperties (files) or BlobPrefix (folders)
        try:
            items = list(container_client.walk_blobs(name_starts_with=current_prefix, delimiter='/'))
        except Exception as e:
            print(f"Error listing blobs: {e}")
            input("Press Enter to return...")
            return

        # Separate folders and files
        dir_list = []
        file_list = []
        
        for item in items:
            if hasattr(item, 'name'):
                name = item.name
                # Remove prefix for display
                rel_name = name[len(current_prefix):]
                
                if name.endswith('/'):
                    dir_list.append(rel_name)
                else:
                    file_list.append(rel_name)

        # Display Navigation
        print("[0] .. (Go Up)")
        
        menu_map = {}
        idx = 1
        
        print("\n[ FOLDERS ]")
        for d in dir_list:
            print(f"[{idx}] {d}")
            menu_map[idx] = {'type': 'dir', 'path': current_prefix + d}
            idx += 1
            
        print("\n[ FILES ]")
        for f in file_list:
            print(f"[{idx}] {f}")
            menu_map[idx] = {'type': 'file', 'path': current_prefix + f}
            idx += 1
            
        print("\n[ COMMANDS ]")
        print("[d] Inspect Current Folder as Delta Table")
        print("[q] Return to Container List")
        
        choice = input("\nSelect: ").strip().lower()
        
        if choice == 'q':
            return
        elif choice == '0':
            # Go Up
            if not current_prefix:
                return # Already at root, act as Back
            # Remove last segment
            # e.g. "a/b/" -> "a/"
            parts = current_prefix.rstrip('/').split('/')
            if len(parts) <= 1:
                current_prefix = ""
            else:
                current_prefix = "/".join(parts[:-1]) + "/"
                
        elif choice == 'd':
            # Inspect current path as Delta
            # Remove trailing slash for load_delta
            path_to_inspect = current_prefix.rstrip('/')
            if not path_to_inspect:
                print("Cannot inspect root as Delta Table directly here.")
                input("Press Enter...")
            else:
                # Ask regarding row count? Default 5
                inspect_delta_table(container_name, path_to_inspect)
                
        elif choice.isdigit():
            idx_choice = int(choice)
            if idx_choice in menu_map:
                item = menu_map[idx_choice]
                if item['type'] == 'dir':
                    # Ask intention
                    print(f"\nSelected Directory: {item['path']}")
                    print("1. Navigate Into")
                    print("2. Inspect as Delta Table")
                    sub = input("Action [1]: ").strip()
                    if sub == '2':
                        inspect_delta_table(container_name, item['path'].rstrip('/'))
                    else:
                        current_prefix = item['path']
                else:
                    # Is file
                    inspect_file(container_client, item['path'])
            else:
                print("Invalid index.")
                input("Press Enter...")
        else:
            print("Invalid command.")
            input("Press Enter...")


import argparse

def find_delta_tables(container_client) -> List[str]:
    """Scan container for Delta Tables by looking for _delta_log directories."""
    print(f"Scanning {container_client.container_name} for Delta Tables...")
    table_paths = set()
    try:
        # We only need to list identifying files. _delta_log/*.json is a good indicator.
        # However, listing ALL blobs might be slow for massive containers.
        # But for 'audit all', we likely have to.
        # Optimization: Just recursively finding _delta_log folders.
        blobs = container_client.list_blobs()
        for blob in blobs:
            if "_delta_log/" in blob.name and blob.name.endswith(".json"):
                # Path is everything before _delta_log
                path = blob.name.split("_delta_log/")[0].rstrip("/")
                if path:
                    table_paths.add(path)
    except Exception as e:
        print(f"Error scanning container: {e}")
    
    return sorted(list(table_paths))

def audit_all(client: BlobServiceClient):
    """Audit all containers and their Delta Tables."""
    print("--- STARTING FULL AUDIT ---")
    containers = list(client.list_containers())
    
    summary = []
    
    for c in containers:
        print(f"\n[Container: {c.name}]")
        container_client = client.get_container_client(c.name)
        tables = find_delta_tables(container_client)
        
        if not tables:
            print("  No Delta Tables found.")
            continue
            
        for t in tables:
            print(f"  Found Table: {t}")
            try:
                df = delta_core.load_delta(c.name, t)
                if df is not None:
                    cols = df.columns.tolist()
                    shape = df.shape
                    print(f"    Shape: {shape}")
                    print(f"    Columns: {cols}")
                    summary.append({
                        "container": c.name,
                        "table": t,
                        "rows": shape[0],
                        "cols": shape[1],
                        "columns": str(cols)
                    })
                else:
                    print("    (Empty or Load Failed)")
            except Exception as e:
                print(f"    Error loading: {e}")
                
    print("\n--- AUDIT SUMMARY ---")
    df_summary = pd.DataFrame(summary)
    if not df_summary.empty:
        print(df_summary.to_string(index=False))
    else:
        print("No tables successfully audited.")

def main():
    parser = argparse.ArgumentParser(description="ADLS Inspector & Auditor")
    parser.add_argument("--audit", action="store_true", help="Audit all containers and columns non-interactively")
    args = parser.parse_args()

    try:
        client = get_service_client()
        
        if args.audit:
            audit_all(client)
            return

        while True:
            clear_screen()
            container = select_container(client)
            if not container:
                break
            navigate_container(client, container)
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"\nCritical Error: {e}")

if __name__ == "__main__":
    main()
