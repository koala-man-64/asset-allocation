# Testing Infrastructure & Standards
**Key Agents**: Architect, Developer, QA Tester, Bookkeeper

## Overview
This document outlines the standardized testing framework for the `AssetAllocation` project. We have moved away from siloed test scripts to a centralized `pytest` architecture to ensure scalability and maintainability.

## Architecture

### 1. Directory Structure
All tests are located in the `tests/` directory at the project root.

```
c:\Users\rdpro\Projects\AssetAllocation\
├── pytest.ini              # Global test runner configuration
├── tests/
│   ├── conftest.py         # Shared fixtures (Azure clients, temp files)
│   ├── unit/               # Fast, isolated logic tests (Mocked I/O)
│   ├── integration/        # Tests interacting with real services (Azure, API)
│   └── e2e/                # Critical user journey flows
```

### 2. Configuration (`pytest.ini`)
- **Root**: Project root acts as the python path base.
- **Discovery**: Looks for tests inside `tests/`.
- **Logging**: CLI logging is enabled at `INFO` level for observability.
- **Markers**:
    - `manual`: Tests that do not run automatically (e.g., persistent data generation).

### 3. Key Fixtures (`tests/conftest.py`)
- **`azure_client`**: (Session-scoped)
    - Initializes an authenticated `BlobStorageClient` using the local `.env`.
    - Skips dependent tests if `AZURE_STORAGE_CONNECTION_STRING` is missing.
- **`temp_test_file`**: (Function-scoped)
    - Provides a unique filename for testing uploads.
    - Automatically cleans up (deletes) the file from Azure after the test completes.

## Usage

### Prerequisites
Ensure your environment is set up with the required testing dependencies:
```bash
pip install -r requirements.txt
```
*(Key requirements: `pytest`, `azure-storage-blob`, `python-dotenv`)*

### Running All Tests
```bash
pytest
```

### Running integration Tests Only
```bash
pytest tests/integration
```

### Running a Specific Test File
```bash
pytest tests/integration/test_market_data_integration.py
```

## Developer Guidelines
1.  **New Tests**: Place new tests in `tests/unit` or `tests/integration` depending on their nature.
2.  **Naming**: File names must start with `test_`.
3.  **Fixtures**: Use `conftest.py` fixtures for common infrastructure rather than re-instantiating clients.

## Current Coverage
- **Azure Blob Storage**: `tests/integration/test_market_data_integration.py` validates connectivity, CRUD operations, and config loading.
