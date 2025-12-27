# Refactoring Report: AssetAllocation

## 1. Executive Summary
The `aaa_ai.py` monolithic script has been refactored into a modular Python package `asset_allocation`. This change addresses critical maintainability and testability issues identified during code review. The logic is now decoupled from the user interface, and hardcoded paths have been moved to a central configuration file.

## 2. New Architecture

### Package Structure
The application is now structured as follows:

```text
AssetAllocation/
├── main.py                    # Entry point
├── asset_allocation/          # Main Package
│   ├── config.py              # Configuration (Paths, Colors, Constants)
│   ├── core/                  
│   │   ├── analysis.py        # Pure Logic (Financial Indicators)
│   │   └── processing.py      # Dataframe Orchestration (Dask/Pandas)
│   ├── data/
│   │   └── storage.py         # Persistence (Load/Save Logic)
│   └── ui/
│       └── cli.py             # User Interface (Menus, Prompts)
└── artifacts/                 # Documentation
```

## 3. Key Improvements

### 3.1. Separation of Concerns
*   **Decoupled Logic:** Calculations (SMA, Bollinger, etc.) in `core/analysis.py` no longer depend on `input()` or `print()`. They accept dataframes/parameters and return dataframes.
*   **UI Layer:** All user interaction is contained within `ui/cli.py`.
*   **Storage Layer:** Persistence logic is isolated in `data/storage.py`.

### 3.2. Configuration Management
*   Global constants, colors, and directory paths are defined in `config.py`.
*   Hardcoded paths (e.g., `G:/My Drive/...`) are replaced with relative paths derived from the package location.

## 4. Usage

To run the application:

```bash
python main.py
```

## 5. Verification
A verification script `test_import.py` is included to ensure all modules import correctly.
Manual verification of the "Add SMA", "Add Bollinger", and "Save/Load" workflows confirms that the refactored code maintains the original functionality.
