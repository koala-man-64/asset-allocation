"""
PCA Analysis Entry Point.
Orchestrates data loading, feature processing, and PCA calculation using modular components.
"""
import os
import sys
import pandas as pd
from sklearn.decomposition import PCA

# Add project root to sys.path if not present (for standalone execution)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Internal module imports (newly modularized)
from scripts.factor_analysis.processor import add_technical_indicators
from scripts.common.core import load_csv, store_csv, write_line
from scripts.common import config as cfg

def main():
    write_line("Starting PCA Analysis Script (Modularized)...")
    
    # 1. Load Data (Strictly Azure)
    input_file = 'get_historical_data_output.csv'
    
    try:
        df = load_csv(input_file)
    except RuntimeError as e:
        write_line(f"CRITICAL ERROR: {e}")
        return
    except Exception as e:
        write_line(f"Error loading data: {e}")
        return

    if df is None or df.empty:
        write_line("No data loaded.")
        return

    # Apply Debug Filter
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        write_line(f"⚠️ DEBUG MODE: Restricting analysis to {len(cfg.DEBUG_SYMBOLS)} symbols: {cfg.DEBUG_SYMBOLS}")
        df = df[df['Symbol'].isin(cfg.DEBUG_SYMBOLS)]


    # 2. Process Features
    write_line("Calculating Technical Indicators...")
    df_processed = add_technical_indicators(df)
    
    # 3. PCA Analysis
    if not df_processed.empty:
        target_features = ['F-Score', 'Standardized_MACD', 'RSI', 'CCI'] 
        
        # Ensure features exist
        available_features = [f for f in target_features if f in df_processed.columns]
        
        if len(available_features) > 1:
            write_line(f"Running PCA on features: {available_features}")
            pca_data = df_processed[available_features].dropna()
            
            if not pca_data.empty:
                pca = PCA(n_components=min(4, len(available_features)))
                pca.fit(pca_data)
                
                loadings = pd.DataFrame(
                    pca.components_.T, 
                    columns=[f'PC{i+1}' for i in range(pca.n_components_)], 
                    index=available_features
                )
                print("\nPCA Loadings:")
                print(loadings)

                # Save results to Azure
                output_path = 'pca_results.csv'
                store_csv(loadings, output_path)
            else:
                write_line("Not enough data for PCA after dropping NaNs.")
        else:
            write_line("Not enough features for PCA.")

    write_line("Analysis complete.")

if __name__ == "__main__":
    main()
