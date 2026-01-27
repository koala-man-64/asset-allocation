# Azure Resource & Secrets Audit

**Date**: 2026-01-21
**Scope**: `asset-allocation` Repository & Azure Subscription
**Auditor**: Architecture Review Agent

---

## 1. Executive Summary

The project uses a **Serverless Container Architecture** on Azure Container Apps (ACA).
*   **Compute**: 2 Services (API, UI) + ~13 Scheduled/Manual Jobs (Data Pipeline).
*   **Data**: Azure Blob Storage (Data Lake) partitioned by Bronze/Silver/Gold/Platinum tiers.
*   **Security**: GitHub Secrets are correctly injected into ACA as "Secrets" and referenced via `secretRef`.
*   **Identity**: Managed Identity is enabled but underutilized; the application primarily uses Connection Strings for storage access.

---

## 2. Resource Map

### Infrastructure & Data
| Resource Type | Resource Name | Role |
| -- | -- | -- |
| **Resource Group** | `AssetAllocationRG` | Logical Grouping |
| **Managed Environment** | `asset-allocation-env` | Container App Host |
| **Container Registry** | `assetallocationacr` | Docker Image Store |
| **Storage Account** | `assetallocstorage001` | Data Lake (ADLS Gen2) |

### Compute: Long-Running Services
| Service Name | Image | Ingress | Port | Auth Mode |
| -- | -- | -- | -- | -- |
| `asset-allocation-ui` | `asset-allocation-ui` | External | 80 | None (Static) |
| `asset-allocation-api` | `asset-allocation-api` | Configurable | 8000 | OIDC / API Key |

### Compute: Data Pipeline Jobs
| Tier | Jobs | Trigger |
| -- | -- | -- |
| **Bronze** | `market`, `finance`, `price-target`, `earnings` | Cron (Daily) |
| **Silver** | `market`, `finance`, `price-target`, `earnings` | Manual / Event |
| **Gold** | `market`, `finance`, `price-target`, `earnings` | Manual / Event |
| **Platinum** | `platinum-ranking-job` | Manual |

---

## 3. GitHub Secrets Audit

Secrets are stored in GitHub Actions and passed to Azure resources during deployment.

| Secret Name | Usage | Description | Security Check |
| -- | -- | -- | -- |
| `AZURE_CREDENTIALS` | `azure/login` | OIDC Federation Service Principal | ✅ **Secure** |
| *(none)* | ACA System Managed Identity + `AcrPull` | Registry Pull Credentials | ✅ **Secure** (RBAC, no secret) |
| `AZURE_STORAGE_CONNECTION_STRING` | ACA `secretRef` | Blob Storage Access Key | ⚠️ **Rotate** (Prefer Managed Identity) |
| `API_KEY` | ACA `secretRef` | API Backend Authentication | ✅ **Secure** (Injected as Secret) |
| `YAHOO_USERNAME` | ACA `secretRef` | External Data Provider Creds | ✅ **Secure** (Injected as Secret) |
| `YAHOO_PASSWORD` | ACA `secretRef` | External Data Provider Creds | ✅ **Secure** (Injected as Secret) |
| `API_AUTH_MODE` | Env Var | Auth toggle (oidc/api_key) | ℹ️ Config (Non-sensitive) |
| `API_OIDC_*` | Env Vars | OIDC Configuration (Issuer, Audience) | ℹ️ Config (Non-sensitive) |

---

## 4. Findings & Recommendations

### 4.1 Storage Access (Medium Risk)
*   **Finding**: The application uses `AZURE_STORAGE_CONNECTION_STRING` for all data access. Keys must be rotated manually and risk exposure.
*   **Recommendation**: Switch to **Azure Managed Identity**.
    *   Grant the ACA System Assigned Identity `Storage Blob Data Contributor` on `assetallocstorage001`.
    *   Update `az_blob_store.py` to use `DefaultAzureCredential()`.

### 4.2 Traffic Flow (Low Risk)
*   **Finding**: The `asset-allocation-api` is exposed publicly (`external: true` implied by UI needs).
*   **Recommendation**: Ensure `API_AUTH_MODE` is strictly enforced and OIDC configuration remains current.

### 4.3 Container Security
*   **Finding**: Images use `latest` tag in some definitions or dynamic `${IMAGE_TAG}` in others.
*   **Recommendation**: Always pin to specific SHAs or Semantic Versions in production to prevent "drift" on redeploy.

### 4.4 Missing Secrets
*   No **Application Insights** connection string was observed in the secrets map. Ensure observability data is being captured (Logs are currently sent to `LOG_FORMAT: JSON`). 
