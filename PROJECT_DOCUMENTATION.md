# Oracle OPC P6 Data Pipeline — Full Project Documentation

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture & Data Flow](#2-architecture--data-flow)
3. [Project Structure](#3-project-structure)
4. [Configuration Files](#4-configuration-files)
   - 4.1 [config/config.yaml](#41-configconfigyaml)
   - 4.2 [.env.example](#42-envexample)
   - 4.3 [requirements.txt](#43-requirementstxt)
5. [Source Code — Line by Line](#5-source-code--line-by-line)
   - 5.1 [src/utils/logger.py](#51-srcutilsloggerpy)
   - 5.2 [src/utils/config.py](#52-srcutilsconfigpy)
   - 5.3 [src/p6_client.py](#53-srcp6_clientpy)
   - 5.4 [src/extractors/project_extractor.py](#54-srcextractorsproject_extractorpy)
   - 5.5 [src/extractors/cost_extractor.py](#55-srcextractorscost_extractorpy)
   - 5.6 [src/extractors/resource_extractor.py](#56-srcextractorsresource_extractorpy)
   - 5.7 [src/transformers/cost_calculator.py](#57-srctransformerscost_calculatorpy)
   - 5.8 [src/transformers/resource_calculator.py](#58-srctransformersresource_calculatorpy)
   - 5.9 [src/loaders/oracle_loader.py](#59-srcloadersoracle_loaderpy)
   - 5.10 [src/loaders/minio_loader.py](#510-srcloadersminio_loaderpy)
6. [Airflow DAGs — Line by Line](#6-airflow-dags--line-by-line)
   - 6.1 [dags/p6_extract_dag.py](#61-dagsp6_extract_dagpy)
   - 6.2 [dags/p6_transform_load_dag.py](#62-dagsp6_transform_load_dagpy)
   - 6.3 [dags/p6_full_pipeline_dag.py](#63-dagsp6_full_pipeline_dagpy)
7. [SQL Files](#7-sql-files)
   - 7.1 [sql/create_tables.sql](#71-sqlcreate_tablessql)
   - 7.2 [sql/upsert_procedures.sql](#72-sqlupsert_proceduressql)
8. [Docker Setup](#8-docker-setup)
   - 8.1 [docker/Dockerfile](#81-dockerdockerfile)
   - 8.2 [docker/docker-compose.yml](#82-dockerdocker-composeyml)
9. [Jenkins CI/CD](#9-jenkins-cicd)
10. [Tests](#10-tests)
11. [Earned Value — Concepts Reference](#11-earned-value--concepts-reference)
12. [Setup & Run Guide](#12-setup--run-guide)

---

## 1. Project Overview

This project is an **automated data pipeline** that does three things:

1. **Extracts** project, cost, WBS (Work Breakdown Structure), activity, and resource data from the **Oracle Primavera Cloud (OPC) REST API** — Oracle's cloud-hosted project management system (P6).
2. **Transforms** the raw data by computing standard construction/project management metrics: Earned Value (EV) indices like CPI, SPI, EAC, and resource utilization percentages.
3. **Loads** the results into two destinations simultaneously:
   - **Oracle Database** — structured relational tables for querying and reporting
   - **MinIO** — an S3-compatible object store, holding both raw JSON archives and processed Parquet files

**Apache Airflow** schedules and orchestrates all steps as a Directed Acyclic Graph (DAG) running daily. **Docker** packages everything into containers for consistent deployment. **Jenkins** automates testing, building, and deploying those containers via CI/CD.

### Who this is for

Project controls teams, data engineers, and analysts working with Oracle Primavera who need automated daily snapshots of project health metrics loaded into a database for dashboarding or reporting.

---

## 2. Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    ORACLE PRIMAVERA CLOUD (OPC)                  │
│         REST API v2 — Projects, WBS, Activities, Resources       │
└──────────────────────────┬──────────────────────────────────────┘
                           │  OAuth2 (client_credentials)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      src/p6_client.py                            │
│         OPCClient — authenticated, paginated HTTP requests       │
└──────┬──────────────────┬─────────────────────┬─────────────────┘
       │                  │                     │
       ▼                  ▼                     ▼
  ProjectExtractor   CostExtractor       ResourceExtractor
  (projects, WBS,   (EV / budget        (resources, roles,
   activities)       cost fields)        assignments)
       │                  │                     │
       └──────────────────┴─────────────────────┘
                          │
                          ▼
             MinIOLoader.upload_raw()
             p6-raw/YYYY/MM/DD/<entity>.json
                          │
                          ▼
              ┌───────────────────────┐
              │      TRANSFORM        │
              │  cost_calculator.py   │──→ CPI, SPI, EAC, VAC, TCPI
              │  resource_calculator.py│──→ Utilization %, Over-allocation
              └───────────┬───────────┘
                          │
           ┌──────────────┴──────────────┐
           ▼                             ▼
   OracleLoader.upsert_*()     MinIOLoader.upload_processed()
   P6_COST_METRICS             p6-processed/YYYY/MM/DD/<entity>.parquet
   P6_RESOURCE_METRICS         (Parquet columnar format)
   P6_PROJECTS / P6_WBS / etc.
           │
           ▼
   Oracle DB (PKG_P6_PIPELINE MERGE procedures)

All steps orchestrated by Apache Airflow DAGs → scheduled 02:00 UTC daily
```

**Why two destinations?**
- **Oracle DB**: Structured queries, joins, reporting tools (Power BI, APEX, SQL)
- **MinIO raw zone**: Immutable archive of what P6 returned each day — audit trail, reprocessing
- **MinIO processed zone**: Parquet is a columnar format ideal for analytical tools (Spark, Pandas, Athena)

---

## 3. Project Structure

```
Oracle OPC API/
├── src/                        Python source package
│   ├── p6_client.py            OPC REST API client
│   ├── extractors/             Pull data from OPC API
│   │   ├── project_extractor.py
│   │   ├── cost_extractor.py
│   │   └── resource_extractor.py
│   ├── transformers/           Calculate metrics
│   │   ├── cost_calculator.py
│   │   └── resource_calculator.py
│   ├── loaders/                Write data to destinations
│   │   ├── oracle_loader.py
│   │   └── minio_loader.py
│   └── utils/
│       ├── config.py           Read config.yaml + environment variables
│       └── logger.py           JSON structured logging
├── dags/                       Apache Airflow DAG definitions
│   ├── p6_extract_dag.py       Extract-only DAG (01:00 UTC)
│   ├── p6_transform_load_dag.py Transform+load DAG (triggered)
│   └── p6_full_pipeline_dag.py Combined daily DAG (02:00 UTC)
├── sql/
│   ├── create_tables.sql       Oracle DDL — table definitions
│   └── upsert_procedures.sql   PL/SQL MERGE package
├── config/
│   ├── config.yaml             Non-secret settings
│   └── airflow_connections.yaml Airflow connection reference
├── docker/
│   ├── Dockerfile              Custom Airflow container image
│   └── docker-compose.yml      Full stack definition
├── jenkins/
│   └── Jenkinsfile             CI/CD pipeline stages
├── tests/
│   ├── test_p6_client.py
│   ├── test_transformers.py
│   └── test_loaders.py
├── requirements.txt            All Python dependencies
├── .env.example                Secrets template
└── setup.py                    Python package installer
```

---

## 4. Configuration Files

### 4.1 `config/config.yaml`

This file holds all **non-secret** configuration. Secrets (passwords, API keys) live in `.env`.

```yaml
opc:
  page_size: 500       # How many records to fetch per API call before paging
  request_timeout_seconds: 30
  max_retries: 3
  project_fields:      # Exact OPC field names to request from /projects endpoint
    - ObjectId         # Internal P6 unique ID for the project
    - BudgetAtCompletion
    - EarnedValueCost  # Total EV across project
    ...

oracle:
  schema: "P6_DATA"    # Oracle schema where all tables live
  pool_min: 2          # Minimum connections kept alive in pool
  pool_max: 10         # Maximum simultaneous connections

minio:
  raw_bucket: "p6-raw"
  processed_bucket: "p6-processed"

pipeline:
  project_filter_status: ["Active", "What-if"]   # Only extract these project statuses
  eac_method: "BAC_CPI"                           # How to calculate Estimate at Completion
```

### 4.2 `.env.example`

Template showing every environment variable the system needs. Copy to `.env` and fill in real values. The file is divided into sections:

- `OPC_*` — Oracle Primavera Cloud API credentials (OAuth2 client ID/secret, tenant URLs)
- `ORACLE_*` — Oracle Database host, port, service name, user, password
- `MINIO_*` — MinIO endpoint and credentials
- `AIRFLOW__*` — Airflow internal settings (Fernet key for encryption, DB connection string)

### 4.3 `requirements.txt`

| Package | Purpose |
|---------|---------|
| `requests` | HTTP client for API calls |
| `requests-oauthlib` | OAuth2 flow on top of requests |
| `python-oracledb` | Oracle DB driver — thin mode (no Oracle Instant Client needed) |
| `minio` | MinIO Python client |
| `apache-airflow` | Workflow orchestration engine |
| `pandas` | Data manipulation; DataFrame ↔ Parquet conversion |
| `pyarrow` | Parquet read/write engine used by pandas |
| `PyYAML` | Parse `config.yaml` |
| `python-dotenv` | Load `.env` file into environment |
| `tenacity` | Retry logic with exponential backoff |
| `python-json-logger` | Emit log lines as JSON objects |
| `pytest` / `pytest-mock` | Unit testing framework |
| `responses` | Mock HTTP calls in tests |
| `flake8` / `black` | Code style linting |

---

## 5. Source Code — Line by Line

---

### 5.1 `src/utils/logger.py`

**Purpose:** Provide a single, consistent logger factory used everywhere in the project. All logs are emitted as JSON, which makes them machine-parseable by log aggregators (Elasticsearch, CloudWatch, etc.).

```python
import logging        # Python's built-in logging module
import sys            # Used to write logs to stdout (so Docker captures them)
from pythonjsonlogger import jsonlogger  # Third-party: formats log records as JSON
```

#### Function: `get_logger(name, level)`

```python
def get_logger(name: str, level: str = "INFO") -> logging.Logger:
```
- **`name`** — typically passed as `__name__`, which evaluates to the module's dotted path (e.g., `src.p6_client`). This appears in the log's `name` field so you know which module produced the log line.
- **`level`** — minimum severity to emit. Defaults to `INFO`. Can be overridden to `DEBUG` for verbose output.

```python
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
```
- `logging.getLogger(name)` either creates a new logger or returns the existing one with that name. Python's logging system is global, so calling this twice with the same name returns the same object.
- The `if logger.handlers` guard prevents adding a second handler if the logger was already configured. Without this, each import of a module would add another handler and every log line would be printed multiple times.

```python
    handler = logging.StreamHandler(sys.stdout)
```
- Creates a handler that writes to **stdout**. In Docker, stdout is captured and forwarded to the container's log driver.

```python
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
```
- Configures JSON output format. `fmt` tells the formatter which standard fields to include. The `%()s` placeholders are Python logging's built-in record attributes. The result looks like:
  ```json
  {"asctime": "2026-01-15T10:30:00", "name": "src.p6_client", "levelname": "INFO", "message": "OPC token acquired", "expires_at": 1737000000}
  ```
- Any `extra={}` dict passed to a log call (e.g., `logger.info("msg", extra={"count": 5})`) appears as additional keys in the JSON.

```python
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
```
- `getattr(logging, "INFO")` resolves the string `"INFO"` to the integer constant `20`. The third argument `logging.INFO` is the fallback if an invalid level string is given.

```python
    logger.propagate = False
```
- Prevents the log record from bubbling up to the root logger (which Airflow also configures). Without this, log lines would appear twice in Airflow's logs.

---

### 5.2 `src/utils/config.py`

**Purpose:** Single place to read settings. Merges non-secret values from `config.yaml` with secrets from environment variables. Returns ready-to-use config dicts for each subsystem.

```python
from functools import lru_cache   # Caches function results — avoids re-reading the file on every call
from pathlib import Path          # Cross-platform file path handling
```

```python
load_dotenv()
```
- Called at module import time. Reads the `.env` file from the current working directory and injects every `KEY=VALUE` line into `os.environ`. If the key already exists in the environment (set by Docker or CI), it is **not** overwritten — real environment variables take priority.

```python
_DEFAULT_CONFIG_PATH = Path(__file__).parents[2] / "config" / "config.yaml"
```
- `__file__` is the path to this file: `src/utils/config.py`
- `.parents[2]` goes up two levels to the project root
- Appends `config/config.yaml` to get the absolute path
- This means the config is always found regardless of where Python is run from

#### Function: `load_config(config_path)`

```python
@lru_cache(maxsize=1)
def load_config(config_path: str | None = None) -> dict[str, Any]:
```
- `@lru_cache(maxsize=1)` means the first call reads and parses the YAML file. Every subsequent call returns the **same cached dict** without re-reading the disk. This is important in Airflow where DAG functions are called frequently.
- `maxsize=1` keeps only one result cached (there's only one config file).

```python
    with open(path, "r") as f:
        return yaml.safe_load(f)
```
- `yaml.safe_load` is used instead of `yaml.load` to prevent arbitrary Python object instantiation from YAML (a security best practice).

#### Function: `get_opc_config()`

```python
def get_opc_config() -> dict[str, Any]:
    cfg = load_config()
    return {
        "base_url": os.environ["OPC_BASE_URL"],   # Square brackets raise KeyError if missing
        "client_secret": os.environ["OPC_CLIENT_SECRET"],
        "scope": os.environ.get("OPC_SCOPE", ""),  # .get() returns "" if not set (optional)
        "timeout": cfg["opc"]["request_timeout_seconds"],  # From YAML
        "fields": cfg["opc"],   # The entire opc: section, including all field lists
    }
```
- Uses `os.environ["KEY"]` (raises `KeyError` if missing) for **required** secrets — fast-fail at startup rather than a cryptic error later.
- Uses `os.environ.get("KEY", default)` for **optional** settings.
- Mixes YAML values (non-secret settings like timeouts) with env vars (secrets).

`get_oracle_config()` and `get_minio_config()` follow the same pattern for their respective subsystems.

---

### 5.3 `src/p6_client.py`

**Purpose:** The only file that talks to the Oracle OPC API. All other code calls methods on this class. It handles authentication, token refresh, HTTP retries, and pagination — so the rest of the codebase just calls `get_projects()` and gets a list back.

#### Imports

```python
from requests_oauthlib import OAuth2Session          # OAuth2-aware requests session
from oauthlib.oauth2 import BackendApplicationClient # OAuth2 grant type: client_credentials
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
```
- `BackendApplicationClient` is the OAuth2 "machine-to-machine" grant type — no user login needed, just client ID and secret. This is the standard for server-to-server API access.
- `tenacity` decorators are applied to `_get()` to automatically retry on transient network errors.

#### Class: `OPCClient`

```python
class OPCClient:
    """Thread-safe OPC REST API v2 client with auto token refresh and pagination."""
```

##### `__init__(self, config)`

```python
def __init__(self, config: dict[str, Any] | None = None):
    self._cfg = config or get_opc_config()   # Use provided config or load from env
    self._base_url = self._cfg["base_url"].rstrip("/")  # Remove trailing slash to avoid double-slash in URLs
    self._timeout = self._cfg["timeout"]     # Per-request timeout in seconds
    self._page_size = self._cfg["page_size"] # Records per paginated page
    self._session = self._create_session()   # Create authenticated HTTP session immediately
```
- The underscore prefix (`_cfg`, `_session`, etc.) is a Python convention meaning "private — don't access from outside the class".

##### `_create_session(self)` — Authentication

```python
def _create_session(self) -> OAuth2Session:
    client = BackendApplicationClient(client_id=self._cfg["client_id"])
```
- `BackendApplicationClient` represents the OAuth2 "client_credentials" flow. It doesn't require a user login — the application itself authenticates with its client ID and secret.

```python
    session = OAuth2Session(client=client)
    self._fetch_token(session)
    return session
```
- `OAuth2Session` is a subclass of `requests.Session`. It automatically attaches the Bearer token to every request. After fetching the token, this session is ready to make authenticated API calls.

##### `_fetch_token(self, session)` — Token Acquisition

```python
def _fetch_token(self, session: OAuth2Session) -> None:
    token = session.fetch_token(
        token_url=self._cfg["token_url"],       # https://idcs-<tenant>.identity.oraclecloud.com/oauth2/v1/token
        client_id=self._cfg["client_id"],
        client_secret=self._cfg["client_secret"],
        scope=self._cfg["scope"] or None,        # None means "request all available scopes"
    )
    logger.info("OPC token acquired", extra={"expires_at": token.get("expires_at")})
```
- Makes a POST to Oracle Identity Cloud Service (IDCS) with client credentials.
- Oracle returns a JSON response with `access_token`, `token_type: "Bearer"`, and `expires_in`.
- The token is stored inside `session.token`. Every subsequent `session.get()` call automatically includes `Authorization: Bearer <token>`.

##### `_ensure_token(self)` — Token Refresh

```python
def _ensure_token(self) -> None:
    if self._session.token.get("expires_in", 0) < 60:
        self._fetch_token(self._session)
```
- OPC access tokens expire (typically after 1 hour). Before every API call, this checks if the token has less than 60 seconds remaining. If so, it fetches a fresh token.
- `expires_in` is the number of seconds until expiration. The `or 0` handles the case where the field is missing.

##### `_get(self, endpoint, params)` — HTTP GET with Retry

```python
@retry(
    reraise=True,                                          # Re-raise the original exception after retries exhausted
    stop=stop_after_attempt(3),                            # Try at most 3 times
    wait=wait_exponential(multiplier=2, min=2, max=30),    # Wait 2s, then 4s, then 8s... up to 30s
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),  # Only retry on network errors
)
def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    self._ensure_token()
    url = f"{self._base_url}/{endpoint.lstrip('/')}"  # lstrip removes leading slash to avoid "//projects"
    response = self._session.get(url, params=params, timeout=self._timeout)
    response.raise_for_status()  # Raises HTTPError for 4xx/5xx status codes
    return response.json()       # Parse JSON body into a Python dict
```
- The `@retry` decorator wraps the function. If a `Timeout` or `ConnectionError` occurs, tenacity catches it, waits, and calls the function again. After 3 failures it re-raises.
- `raise_for_status()` converts HTTP error responses (401, 403, 404, 500) into Python exceptions, so the caller doesn't have to check status codes manually.
- HTTP 401 (invalid token) and 403 (forbidden) are **not** retried — they are configuration problems, not transient failures.

##### `_paginate(self, endpoint, fields, extra_filter)` — Pagination Generator

```python
def _paginate(self, endpoint, fields, extra_filter=None) -> Generator[list[...], None, None]:
    offset = 0
    field_str = ",".join(fields)    # OPC API wants: "ObjectId,Name,Status" as a comma-separated string
    while True:
        params = {
            "fields": field_str,
            "limit": self._page_size,   # e.g. 500 — number of records per page
            "offset": offset,           # e.g. 0, 500, 1000 — where the page starts
        }
        if extra_filter:
            params["filter"] = extra_filter   # e.g. "Status IN (\"Active\")"
        data = self._get(endpoint, params=params)
        records = data if isinstance(data, list) else data.get("items", [])
```
- The OPC API returns either a plain JSON array `[{...}, {...}]` or an object with an `items` key `{"items": [...], "count": 500}`. The `isinstance` check handles both formats.

```python
        if not records:
            break    # No more records — stop the loop
        yield records   # Hand this page to the caller (lazy evaluation)
        if len(records) < self._page_size:
            break    # Partial page means we're at the last page
        offset += self._page_size   # Advance to next page
```
- `yield` makes this a **generator function**. It pauses execution and hands the current page to the caller. This means large datasets don't have to fit in memory all at once — pages are fetched one at a time.
- The "partial page" check (`len(records) < self._page_size`) is more efficient than making an extra API call that returns zero records.

##### `_get_all(self, endpoint, fields, extra_filter)` — Collect All Pages

```python
def _get_all(self, endpoint, fields, extra_filter=None) -> list[dict]:
    result: list[dict] = []
    for page in self._paginate(endpoint, fields, extra_filter):
        result.extend(page)    # Flatten pages into a single list
    logger.info("Fetched records", extra={"endpoint": endpoint, "count": len(result)})
    return result
```
- Drives the pagination generator to completion, collecting every record into one list.

##### Public Methods

Each public method calls `_get_all()` with the appropriate endpoint and a filter. Examples:

```python
def get_projects(self, status_filter=None):
    fields = self._cfg["fields"]["project_fields"]   # Field list from config.yaml
    f = None
    if status_filter:
        statuses = ",".join(f'"{s}"' for s in status_filter)  # Format: "Active","What-if"
        f = f"Status IN ({statuses})"                          # OPC filter syntax
    return self._get_all("projects", fields, extra_filter=f)

def get_wbs(self, project_object_id: int):
    # Filter WBS records to only those belonging to this specific project
    return self._get_all("wbs", fields, extra_filter=f"ProjectObjectId = {project_object_id}")

def get_resource_assignments(self, project_object_id: int):
    # The OPC endpoint for resource-activity links
    return self._get_all("activityresourceassignments", fields, ...)
```

##### Context Manager Support

```python
def __enter__(self) -> "OPCClient":
    return self

def __exit__(self, *_: Any) -> None:
    self.close()    # Closes the underlying requests.Session

def close(self) -> None:
    self._session.close()
```
- Enables the `with OPCClient(...) as client:` pattern. This guarantees the HTTP session is closed even if an exception occurs, releasing connection resources.

---

### 5.4 `src/extractors/project_extractor.py`

**Purpose:** Thin orchestration layer that calls `OPCClient` methods and collects project structural data (projects, WBS, activities).

#### Class: `ProjectExtractor`

```python
def __init__(self, client: OPCClient, status_filter: list[str] | None = None):
    self._client = client
    self._status_filter = status_filter or ["Active", "What-if"]
```
- Accepts an already-authenticated `OPCClient` — this class doesn't manage auth.
- Defaults to extracting "Active" and "What-if" projects only, filtering out historical/closed projects.

##### `extract_projects(self)`

```python
def extract_projects(self) -> list[dict]:
    logger.info("Extracting projects", extra={"status_filter": self._status_filter})
    projects = self._client.get_projects(self._status_filter)
    logger.info("Projects extracted", extra={"count": len(projects)})
    return projects
```
- Calls `OPCClient.get_projects()` which handles all pagination automatically.
- Logs both before (so you can see the filter being applied) and after (so you know how many were returned).

##### `extract_wbs(self, project_ids)`

```python
def extract_wbs(self, project_ids: list[int]) -> list[dict]:
    all_wbs: list[dict] = []
    for pid in project_ids:          # Loop through each project
        wbs = self._client.get_wbs(pid)
        all_wbs.extend(wbs)          # extend() adds all items from wbs into all_wbs (not nested)
        logger.debug("WBS fetched", extra={"project_id": pid, "wbs_count": len(wbs)})
    logger.info("WBS extraction complete", extra={"total": len(all_wbs)})
    return all_wbs
```
- WBS must be fetched per-project because the OPC API filters by project. There is no single endpoint that returns all WBS across all projects.
- The result is a flat list — WBS items from all projects merged together. Each record contains a `ProjectObjectId` field to link back to its project.

##### `extract_activities(self, project_ids)` — Same pattern as WBS

##### `extract_all(self)` — Convenience Method

```python
def extract_all(self) -> dict[str, list[dict]]:
    projects = self.extract_projects()
    project_ids = [p["ObjectId"] for p in projects]  # List comprehension: get just the IDs
    wbs = self.extract_wbs(project_ids)
    activities = self.extract_activities(project_ids)
    return {"projects": projects, "wbs": wbs, "activities": activities}
```
- Runs all three extractions and returns them as a single dictionary. Useful when you need all three in one call.

---

### 5.5 `src/extractors/cost_extractor.py`

**Purpose:** Filter and reshape raw project/activity data to expose only cost-relevant fields.

#### Module-level constant

```python
_EV_SPREAD_FIELDS = [
    "ProjectObjectId", "StartDate", "FinishDate",
    "PlannedValue", "EarnedValue", "ActualCost", ...
]
```
- Defined at module level (prefixed with `_` meaning private to this module). This is the list of EV spreadsheet fields available from OPC for time-phased data.

#### Class: `CostExtractor`

##### `extract_project_cost(self, projects)`

```python
def extract_project_cost(self, projects: list[dict]) -> list[dict]:
    cost_fields = ["ObjectId", "Id", "Name", "DataDate", "TotalBudgetedCost", ...]
    records = []
    for p in projects:
        record = {k: p.get(k) for k in cost_fields}  # Dict comprehension: keep only cost fields
        records.append(record)
    return records
```
- Takes already-fetched project records (from `ProjectExtractor`) and produces a trimmed version containing only the cost/EV columns.
- `p.get(k)` returns `None` if a field is absent — safer than `p[k]` which raises `KeyError`.
- The raw project records from OPC contain many fields (50+). This produces a focused subset for the cost pipeline.

##### `extract_activity_cost(self, activities)`

- Same pattern — filters activity records down to cost-relevant fields: planned cost, actual cost, BAC, EV, percent complete, and dates.

---

### 5.6 `src/extractors/resource_extractor.py`

**Purpose:** Extract the three resource-related entities: resource master data, roles, and resource-activity assignments.

#### Class: `ResourceExtractor`

##### `extract_resources(self)`

```python
def extract_resources(self) -> list[dict]:
    resources = self._client.get_resources()
    logger.info("Resources extracted", extra={"count": len(resources)})
    return resources
```
- `get_resources()` fetches from the OPC `/resources` endpoint — the master list of people, equipment, and materials defined in the system.
- Unlike projects/WBS/activities, resources are **not** filtered by project — they are global to the tenant.

##### `extract_roles(self)`

```python
def extract_roles(self) -> list[dict]:
    roles = self._client.get_roles()
    ...
```
- Roles are job function templates (e.g., "Project Manager", "Civil Engineer"). Resources are assigned to roles. Fetches from `/roles` endpoint.

##### `extract_assignments(self, project_ids)`

```python
def extract_assignments(self, project_ids: list[int]) -> list[dict]:
    all_assignments = []
    for pid in project_ids:
        assignments = self._client.get_resource_assignments(pid)
        all_assignments.extend(assignments)
    return all_assignments
```
- Resource assignments (which resource is assigned to which activity, with planned/actual units and cost) must be fetched per project, the same way WBS is fetched.

##### `extract_all(self, project_ids)`

```python
def extract_all(self, project_ids: list[int]) -> dict:
    return {
        "resources": self.extract_resources(),
        "roles": self.extract_roles(),
        "resource_assignments": self.extract_assignments(project_ids),
    }
```
- Convenience method returning all three in one call.

---

### 5.7 `src/transformers/cost_calculator.py`

**Purpose:** Pure calculation functions. No I/O, no API calls — just math on Python dicts. Takes raw OPC data and produces Earned Value metrics.

#### Earned Value (EV) background

Earned Value Management (EVM) is a project control methodology. The three core inputs are:
- **PV (Planned Value / BCWS)** — what work was planned to cost by this date
- **EV (Earned Value / BCWP)** — budgeted cost of work actually performed
- **AC (Actual Cost / ACWP)** — what the work actually cost
- **BAC (Budget at Completion)** — total approved project budget

#### Helper: `_safe_div(numerator, denominator, default)`

```python
def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator == 0:
        return default       # Prevent ZeroDivisionError when a project hasn't started
    return numerator / denominator
```
- Used for every division. When a project has not started (AC = 0, PV = 0), all ratios are zero rather than crashing.

#### Function: `calculate_project_ev_metrics(projects, eac_method, run_date)`

```python
def calculate_project_ev_metrics(projects, eac_method="BAC_CPI", run_date=None):
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
```
- `run_date` defaults to today in UTC. Always use UTC in data pipelines to avoid timezone confusion.

```python
    for p in projects:
        pv = float(p.get("PlannedValueCost") or 0)
```
- `p.get("PlannedValueCost") or 0` — `.get()` returns `None` if the key is absent, and `or 0` converts `None` to `0`. Then `float()` converts integers/strings to float. This three-step pattern safely handles missing/null values.

```python
        cpi = _safe_div(ev, ac)       # Cost Performance Index: how much value per dollar spent
        spi = _safe_div(ev, pv)       # Schedule Performance Index: how much work per planned work
        cv = ev - ac                  # Cost Variance: positive = under budget
        sv = ev - pv                  # Schedule Variance: positive = ahead of schedule
```

```python
        if eac_method == "AC_PLUS_REMAINING":
            eac = ac + (bac - ev)     # Method 2: assume remaining work will proceed at original budget
        else:
            eac = _safe_div(bac, cpi) if cpi else bac  # Method 1: if you keep spending at current rate
```
- **BAC_CPI** (default): `EAC = BAC / CPI`. If CPI is 0.8, you're getting only 80 cents of value per dollar, so you'll need 25% more money total.
- **AC_PLUS_REMAINING**: Assumes the overrun to date was a one-time event and remaining work will be performed on budget. More optimistic.

```python
        vac = bac - eac               # Variance at Completion: how over/under the final estimate
        tcpi = _safe_div(bac - ev, bac - ac)  # TCPI: performance needed to finish on budget
```
- **TCPI > 1.0** means you need to perform *better* than you have been to finish on budget. Unrealistic if > 1.2.

```python
        cv_pct = _safe_div(cv, bac) * 100  # Cost variance as percentage of BAC
        sv_pct = _safe_div(sv, bac) * 100  # Schedule variance as percentage of BAC
```

The output dict uses **UPPERCASE keys** matching the Oracle DB column names exactly, so it can be passed directly to the Oracle loader without any mapping step.

#### Function: `calculate_activity_ev_metrics(activities, run_date)`

- Same structure but at the activity level. Calculates only CPI and CV (activity-level SPI is less meaningful because activities don't have independent PV values in the same sense).

---

### 5.8 `src/transformers/resource_calculator.py`

**Purpose:** Calculate resource utilization from assignment data.

#### Function: `calculate_assignment_metrics(assignments, run_date)`

```python
for a in assignments:
    planned = float(a.get("PlannedUnits") or 0)
    actual = float(a.get("ActualUnits") or 0)
    remaining = float(a.get("RemainingUnits") or 0)
    planned_cost = float(a.get("PlannedCost") or 0)
    actual_cost = float(a.get("ActualCost") or 0)
```
- "Units" in P6 represent hours for labor resources, or equipment-days for equipment.

```python
    utilization_pct = _safe_div(actual, planned) * 100
```
- `100%` = resource used exactly as planned. `>100%` = over-utilized/over-allocated. `<100%` = under-utilized.

```python
    unit_variance = planned - actual       # Positive = saved hours, Negative = used more than planned
    cost_variance = planned_cost - actual_cost
    over_allocated = actual > planned      # Boolean: True if more hours used than planned
```

```python
    "OVER_ALLOCATED": 1 if over_allocated else 0,  # Store as integer (1/0) for Oracle NUMBER column
```
- Oracle doesn't have a native boolean type. `1` = over-allocated, `0` = not over-allocated.

#### Function: `calculate_resource_summary(assignment_metrics, run_date)`

```python
agg: dict[int, dict] = defaultdict(
    lambda: {
        "total_planned_units": 0.0,
        "assignment_count": 0,
        "over_allocated_count": 0,
        ...
    }
)
```
- `defaultdict` with a `lambda` factory: the first time a resource ID is seen as a key, a fresh accumulator dict is automatically created. No need to check `if rid in agg` before adding.

```python
for m in assignment_metrics:
    rid = m["RESOURCE_OBJECT_ID"]
    agg[rid]["total_planned_units"] += m["PLANNED_UNITS"]
    agg[rid]["assignment_count"] += 1
    agg[rid]["over_allocated_count"] += m["OVER_ALLOCATED"]  # Adds 1 for each over-allocated assignment
```
- Walks through all assignment metrics and sums up values per resource ID.

```python
for rid, data in agg.items():
    utilization = _safe_div(data["total_actual_units"], data["total_planned_units"]) * 100
    summaries.append({
        "RESOURCE_OBJECT_ID": rid,
        "OVERALL_UTILIZATION_PCT": round(utilization, 4),
        "ASSIGNMENT_COUNT": data["assignment_count"],
        "OVER_ALLOCATED_COUNT": data["over_allocated_count"],
        ...
    })
```
- After aggregating, converts each resource's accumulated totals into a summary record.

---

### 5.9 `src/loaders/oracle_loader.py`

**Purpose:** Write data into Oracle Database using MERGE statements (upsert = insert or update). Uses a connection pool for efficiency.

#### Class: `OracleLoader`

##### `__init__(self, config)`

```python
def __init__(self, config=None):
    self._cfg = config or get_oracle_config()
    self._pool: oracledb.ConnectionPool | None = None   # Pool not created until connect() is called
```
- The pool is `None` initially — it's only created when `connect()` or `_get_connection()` is called. This is "lazy initialization": don't open DB connections until you actually need them.

##### `_get_dsn(self)` — Data Source Name

```python
def _get_dsn(self) -> str:
    return f"{self._cfg['host']}:{self._cfg['port']}/{self._cfg['service_name']}"
    # e.g. "db.company.com:1521/ORCL"
```
- Oracle's DSN format: `host:port/service_name`. This is the "thin mode" connect string — no TNS names file or Oracle client needed.

##### `connect(self)`

```python
def connect(self) -> None:
    self._pool = oracledb.create_pool(
        user=..., password=..., dsn=self._get_dsn(),
        min=2,    # Keep 2 connections always open (warm) — avoids connection overhead
        max=10,   # Never exceed 10 simultaneous connections
        increment=1,  # When more connections needed, add one at a time
    )
```
- A connection pool is more efficient than opening/closing a connection per batch. The pool reuses connections and manages their lifecycle.

##### `_get_connection(self)` — Context Manager

```python
@contextmanager
def _get_connection(self) -> Generator[oracledb.Connection, None, None]:
    if not self._pool:
        self.connect()        # Auto-connect if not already connected
    conn = self._pool.acquire()  # Borrow a connection from the pool
    try:
        yield conn            # Hand connection to the caller (the `with` block runs here)
        conn.commit()         # If no exception: commit the transaction
    except Exception:
        conn.rollback()       # If any exception: undo all changes in this batch
        raise                 # Re-raise so the caller knows it failed
    finally:
        self._pool.release(conn)  # Always return connection to pool (whether success or failure)
```
- `@contextmanager` turns this generator function into something usable with `with _get_connection() as conn:`.
- The `try/except/finally` ensures that: success → commit, failure → rollback, always → release connection. This is the safest pattern for transactional database work.

##### `_bulk_execute(self, conn, procedure, records, batch_size=500)`

```python
def _bulk_execute(self, conn, procedure, records, batch_size=500) -> int:
    if not records:
        return 0    # Nothing to do — return immediately
    cursor = conn.cursor()
    keys = list(records[0].keys())   # Get column names from the first record
    for i in range(0, len(records), batch_size):    # Slice into batches of 500
        batch = records[i : i + batch_size]
        data = [[r.get(k) for k in keys] for r in batch]  # Convert list of dicts to list of lists
        cursor.executemany(procedure, data)   # One round-trip for up to 500 rows
```
- `executemany` is dramatically faster than calling `execute` in a loop. A single `executemany` call with 500 rows sends one network packet and lets Oracle process all 500 rows in one operation (array DML).
- `range(0, len(records), batch_size)` produces `[0, 500, 1000, ...]` — the start index of each batch.
- The `data` list converts each record dict into an ordered list of values. Oracle's `:1, :2, :3` bind variables map positionally to this list.

##### Public Upsert Methods

```python
def upsert_projects(self, records: list[dict]) -> int:
    with self._get_connection() as conn:
        return self._bulk_execute(
            conn,
            "BEGIN PKG_P6_PIPELINE.UPSERT_PROJECT(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12); END;",
            records,
        )
```
- Each method calls the corresponding PL/SQL procedure in the `PKG_P6_PIPELINE` package.
- `:1, :2, ...` are Oracle bind variable placeholders. The actual values are passed by `executemany`.
- The `BEGIN ... END;` syntax is how you call a stored procedure in Oracle.

##### Context Manager Support

```python
def __enter__(self) -> "OracleLoader":
    self.connect()    # Open pool when entering `with` block
    return self

def __exit__(self, *_) -> None:
    self.close()      # Close pool when leaving `with` block (success or failure)
```
- Enables: `with OracleLoader() as loader: loader.upsert_projects(records)`

---

### 5.10 `src/loaders/minio_loader.py`

**Purpose:** Store and retrieve data in MinIO using two storage zones: raw JSON (original API data) and processed Parquet (computed metrics).

#### Class: `MinIOLoader`

##### `__init__(self, config)`

```python
def __init__(self, config=None):
    self._cfg = config or get_minio_config()
    self._client = Minio(
        endpoint=self._cfg["endpoint"],    # e.g. "minio:9000" (internal Docker network)
        access_key=self._cfg["access_key"],
        secret_key=self._cfg["secret_key"],
        secure=self._cfg["secure"],        # False for HTTP (internal), True for HTTPS (production)
    )
    self._ensure_buckets()    # Create buckets on first run if they don't exist
```

##### `_ensure_buckets(self)`

```python
def _ensure_buckets(self) -> None:
    for bucket in (self._cfg["raw_bucket"], self._cfg["processed_bucket"]):
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)
            logger.info("Created bucket", extra={"bucket": bucket})
```
- Checks both required buckets on initialization. If they don't exist (first run), creates them automatically. This makes the system self-initializing.

##### `_date_prefix(self, run_date)` — Path Partitioning

```python
def _date_prefix(self, run_date=None) -> str:
    d = run_date or date.today()
    if isinstance(d, str):
        d = date.fromisoformat(d)   # Convert "2026-01-15" string to date object
    return f"{d.year:04d}/{d.month:02d}/{d.day:02d}"  # e.g. "2026/01/15"
```
- Creates a directory-like prefix that partitions data by date. The `:04d` and `:02d` format specifiers zero-pad the numbers (so January is `01`, not `1`).
- This creates a Hive-style partition structure, making it easy to list all files for a specific date and compatible with analytical tools.

##### `upload_raw(self, entity, records, run_date)`

```python
def upload_raw(self, entity, records, run_date=None) -> str:
    prefix = self._date_prefix(run_date)
    object_name = f"{prefix}/{entity}.json"   # e.g. "2026/01/15/projects.json"
    payload = json.dumps(records, default=str).encode("utf-8")
```
- `json.dumps(records, default=str)` serializes the list of dicts to a JSON string. `default=str` means any non-JSON-serializable value (like Python `datetime` objects) is converted to its string representation instead of raising an error.
- `.encode("utf-8")` converts the string to bytes.

```python
    self._client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=io.BytesIO(payload),   # MinIO expects a file-like object, not raw bytes
        length=len(payload),         # Must specify size upfront for streaming
        content_type="application/json",
    )
```
- `io.BytesIO(payload)` wraps the bytes in an in-memory file object. MinIO's client requires a seekable stream.
- `length` must be provided because MinIO needs to set the `Content-Length` HTTP header.

##### `download_raw(self, entity, run_date)`

```python
def download_raw(self, entity, run_date=None) -> list[dict]:
    response = self._client.get_object(self._cfg["raw_bucket"], object_name)
    try:
        data = json.loads(response.read())
    finally:
        response.close()
        response.release_conn()   # Return the underlying HTTP connection to the pool
    return data
```
- `finally` ensures the connection is always released, even if `json.loads` throws a parsing error.

##### `upload_processed(self, entity, records, run_date)`

```python
def upload_processed(self, entity, records, run_date=None) -> str:
    df = pd.DataFrame(records)          # Convert list of dicts to DataFrame
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")  # Write Parquet bytes into in-memory buffer
    buf.seek(0)                          # Rewind buffer to the beginning before reading
    size = buf.getbuffer().nbytes        # Get byte count for Content-Length header
    self._client.put_object(..., data=buf, length=size, ...)
```
- Parquet is a columnar binary format. It compresses much better than CSV/JSON for analytical workloads and preserves data types (numbers stay as numbers, dates stay as dates).
- `index=False` prevents pandas from writing the DataFrame row numbers (0, 1, 2...) as a column in the file.
- `engine="pyarrow"` uses Apache Arrow for Parquet I/O — faster and more type-correct than the default engine.

##### `download_processed(self, entity, run_date)` — Returns `pd.DataFrame`

```python
def download_processed(self, entity, run_date=None) -> pd.DataFrame:
    buf = io.BytesIO(response.read())
    return pd.read_parquet(buf, engine="pyarrow")
```
- Returns a pandas DataFrame directly. The transform-load DAG tasks call `.to_dict(orient="records")` on it to convert back to a list of dicts for the Oracle loader.

---

## 6. Airflow DAGs — Line by Line

Apache Airflow is a platform to schedule and monitor data workflows. A **DAG** (Directed Acyclic Graph) defines a set of tasks and their dependencies. Tasks run in dependency order.

---

### 6.1 `dags/p6_extract_dag.py`

**Purpose:** Run daily at 01:00 UTC. Connect to OPC, extract all data, store raw JSON in MinIO.

```python
default_args = {
    "owner": "data_engineering",
    "retries": 2,               # Retry failed tasks 2 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
    "email_on_failure": False,  # Disable email alerts (configure per org)
}
```
- `default_args` applies to every task in the DAG unless overridden per task.

#### Task: `extract_projects`

```python
def extract_projects(**context) -> None:
    run_date = context["ds"]   # "ds" = execution date string, e.g. "2026-01-15"
```
- `**context` — Airflow passes a context dictionary to every Python callable task. `ds` is the "data interval start" — the logical date this run corresponds to. For a daily DAG, this is yesterday's date (Airflow runs after the period ends).

```python
    with OPCClient(_get_opc_config_from_airflow()) as client:
        extractor = ProjectExtractor(client)
        projects = extractor.extract_projects()

    minio = MinIOLoader()
    minio.upload_raw("projects", projects, run_date=run_date)
    context["ti"].xcom_push(key="project_ids", value=[p["ObjectId"] for p in projects])
```
- `context["ti"]` = Task Instance. `xcom_push` saves a value so downstream tasks can read it.
- The list of project IDs is pushed to XCom so that `extract_wbs`, `extract_activities`, and `extract_assignments` know which projects to query.

#### Task Group Structure

```python
t_extract_projects >> tg_project_details
t_extract_projects >> tg_master
```
- `>>` is Airflow's dependency operator. It means "extract_projects must succeed before these task groups start".
- `tg_project_details` (WBS, activities, assignments) and `tg_master` (resources) run in **parallel** after projects are fetched.

---

### 6.2 `dags/p6_transform_load_dag.py`

**Purpose:** Read raw data from MinIO, compute metrics, write to Oracle and MinIO processed zone. Designed to be **triggered externally** (not scheduled), so it can be called by `p6_extract_dag` or run manually.

```python
with DAG(
    dag_id="p6_transform_load_dag",
    schedule_interval=None,   # None = no automatic schedule; must be triggered
    ...
```
- `schedule_interval=None` makes this a manually-triggered or API-triggered DAG. It won't run on its own.

#### Task: `transform_cost_metrics`

```python
def transform_cost_metrics(**context) -> None:
    run_date = context["ds"]
    minio = MinIOLoader()
    projects = minio.download_raw("projects", run_date=run_date)  # Read what was stored in extract step
    metrics = calculate_project_ev_metrics(projects, eac_method=_eac_method(), run_date=run_date)
    minio.upload_processed("cost_metrics", metrics, run_date=run_date)
```
- Reads the raw project JSON that was saved in the extract step.
- Calculates EV metrics (pure Python math, no API calls).
- Saves results as Parquet in the processed zone.

#### Task Group Flow

```python
tg_raw_load >> tg_transform >> tg_metrics_load
```
1. `tg_raw_load` — loads all raw entities to Oracle in parallel
2. `tg_transform` — calculates metrics (waits for raw loads to complete)
3. `tg_metrics_load` — loads computed metrics to Oracle

---

### 6.3 `dags/p6_full_pipeline_dag.py`

**Purpose:** The primary production DAG. Combines all steps in one place with a daily schedule.

```python
with DAG(
    dag_id="p6_full_pipeline_dag",
    schedule_interval="0 2 * * *",   # Cron: run at 02:00 UTC every day
    start_date=datetime(2024, 1, 1), # Historical start — Airflow uses this to compute missed runs
    catchup=False,                   # Don't backfill from start_date to today on first run
    max_active_runs=1,               # Only one instance of this DAG can run at a time
```
- `catchup=False` is important — without it, Airflow would try to run the DAG for every day from Jan 1, 2024 to today on the first deployment.

#### Helper: `_cfg()` and `_opc_config()`

```python
def _cfg() -> dict:
    config_path = Variable.get("pipeline_config_path", default_var="/opt/airflow/config/config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)
```
- Reads config from Airflow Variable. This allows changing the config path without redeploying the DAG.
- `Variable.get()` reads from Airflow's metadata database — values are set in the Airflow UI.

```python
def _opc_config() -> dict:
    cfg = _cfg()
    return {
        "base_url": Variable.get("opc_base_url"),          # Secrets stored in Airflow Variables
        "client_id": Variable.get("opc_client_id"),
        "client_secret": Variable.get("opc_client_secret"),
        ...
        "timeout": cfg["opc"]["request_timeout_seconds"],  # Non-secrets from config.yaml
    }
```
- Secrets are stored in **Airflow Variables** (encrypted in the Airflow metadata DB), not in code or config files.

#### Extract tasks: XCom communication

```python
def extract_and_store_projects(**context) -> None:
    ...
    context["ti"].xcom_push(key="project_ids", value=[p["ObjectId"] for p in projects])

def extract_and_store_wbs(**context) -> None:
    pids = context["ti"].xcom_pull(task_ids="extract.extract_projects", key="project_ids")
```
- `xcom_push` saves a value to Airflow's XCom store (stored in the metadata DB).
- `xcom_pull` reads it. The `task_ids` argument must match the task ID exactly, including the TaskGroup prefix (`"extract.extract_projects"`).

#### DAG task dependencies

```python
with TaskGroup("extract") as tg_extract:
    t_proj >> [t_wbs, t_act, t_res]   # Fan-out: wbs, activities, and resources all start after projects

with TaskGroup("transform") as tg_transform:
    t_cost       # These run in parallel (no dependency between them)
    t_res_metrics

tg_extract >> tg_transform >> tg_oracle
```
- `tg_extract >> tg_transform` means every task in `tg_extract` must succeed before any task in `tg_transform` starts.
- Within `tg_oracle`, all seven load tasks run in parallel (no dependencies between them).

---

## 7. SQL Files

### 7.1 `sql/create_tables.sql`

#### Table: `P6_PROJECTS`

```sql
CREATE TABLE P6_DATA.P6_PROJECTS (
    OBJECT_ID     NUMBER(18)  NOT NULL,   -- P6's internal unique identifier
    PROJECT_ID    VARCHAR2(50),           -- Human-readable project code (e.g. "PRJ-001")
    STATUS        VARCHAR2(50),           -- "Active", "What-if", "Inactive", etc.
    DATA_DATE     DATE,                   -- P6's "as-of" date for this snapshot
    CPI           NUMBER(10,6),           -- Stored with 6 decimal places for precision
    LOAD_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP,  -- Auto-set on every row to when it was loaded
    CONSTRAINT PK_P6_PROJECTS PRIMARY KEY (OBJECT_ID)  -- ObjectId is the unique key
);
```

#### Table: `P6_COST_METRICS`

```sql
CREATE TABLE P6_DATA.P6_COST_METRICS (
    PROJECT_OBJECT_ID NUMBER(18) NOT NULL,
    RUN_DATE          DATE       NOT NULL,   -- The pipeline's execution date
    ...
    CONSTRAINT PK_P6_COST_METRICS PRIMARY KEY (PROJECT_OBJECT_ID, RUN_DATE)
```
- The primary key is a **composite key** of project + date. This allows storing one EV snapshot per project per day, building a time series for trend analysis.

#### Indexes

```sql
CREATE INDEX IDX_ACT_PROJECT ON P6_DATA.P6_ACTIVITIES(PROJECT_OBJECT_ID);
CREATE INDEX IDX_COST_DATE   ON P6_DATA.P6_COST_METRICS(RUN_DATE);
```
- Indexes speed up queries that filter by project or by date — the most common query patterns.

### 7.2 `sql/upsert_procedures.sql`

#### Package: `PKG_P6_PIPELINE`

Oracle packages group related procedures. The package header (spec) declares what's public; the body implements it.

#### `UPSERT_PROJECT` — The MERGE Pattern

```sql
PROCEDURE UPSERT_PROJECT(p_object_id IN NUMBER, p_project_id IN VARCHAR2, ...) IS
BEGIN
    MERGE INTO P6_DATA.P6_PROJECTS tgt          -- Target table
    USING (SELECT p_object_id AS OBJECT_ID FROM DUAL) src  -- "Virtual" source row from parameters
    ON (tgt.OBJECT_ID = src.OBJECT_ID)          -- Match condition: same ObjectId = same project
    WHEN MATCHED THEN UPDATE SET                -- Row exists: update all columns
        PROJECT_ID = p_project_id,
        STATUS     = p_status,
        ...
        LOAD_TIMESTAMP = SYSTIMESTAMP           -- Always update the load timestamp
    WHEN NOT MATCHED THEN INSERT (...)          -- Row doesn't exist: insert it
    VALUES (...);
END UPSERT_PROJECT;
```
- **MERGE** (also called upsert = update + insert) is the key pattern. It atomically decides: does this project already exist? If yes, update it. If no, insert it. This is idempotent — running the pipeline twice for the same date produces the same result, not duplicates.
- `FROM DUAL` — Oracle's built-in single-row virtual table, used here to create a single-row "source" from the stored procedure parameters.

---

## 8. Docker Setup

### 8.1 `docker/Dockerfile`

```dockerfile
FROM apache/airflow:2.8.4-python3.11
```
- Start from the official Airflow image with Python 3.11. This already has Airflow installed with all its dependencies.

```dockerfile
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
```
- Temporarily switch to root to install system packages.
- `--no-install-recommends` skips optional packages to keep the image small.
- `apt-get clean && rm -rf /var/lib/apt/lists/*` removes the package cache — reduces image size.

```dockerfile
USER airflow
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt
```
- Switch back to the `airflow` user (non-root) for security.
- `--no-cache-dir` prevents pip from writing package caches to the image — reduces image size.

```dockerfile
COPY src/ /opt/airflow/src/
COPY config/ /opt/airflow/config/
COPY dags/ /opt/airflow/dags/
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
```
- Copies source code into the container.
- `PYTHONPATH` is extended to include `/opt/airflow`. This is why `from src.p6_client import OPCClient` works inside the container — Python can find `src/` as a package.

### 8.2 `docker/docker-compose.yml`

```yaml
x-airflow-common: &airflow-common   # YAML anchor — defines shared config
  build:
    context: ..                     # Build context is the project root
    dockerfile: docker/Dockerfile
  env_file: ../.env                 # Load all environment variables from .env
```
- `&airflow-common` defines a YAML anchor. The `<<: *airflow-common` syntax below "inherits" this block — avoiding config duplication across webserver, scheduler, etc.

#### Services

| Service | Purpose |
|---------|---------|
| `postgres` | Airflow's metadata database (stores DAG state, task history, XCom) |
| `airflow-init` | One-time container: runs DB migration and creates admin user, then exits |
| `airflow-webserver` | Serves the Airflow UI on port 8080 |
| `airflow-scheduler` | Reads DAG files, evaluates schedules, queues tasks |
| `minio` | Object storage — exposes S3 API on :9000, web console on :9001 |

```yaml
airflow-webserver:
    healthcheck:
        test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
        interval: 30s
        retries: 5
```
- Docker's health check system periodically calls `curl` against Airflow's health endpoint. If it fails 5 times, Docker marks the container as "unhealthy" and dependent services won't start.

```yaml
depends_on:
    airflow-init:
        condition: service_completed_successfully
```
- `service_completed_successfully` means Docker waits until `airflow-init` exits with code 0 (success) before starting webserver and scheduler. This ensures the DB is migrated before Airflow tries to use it.

---

## 9. Jenkins CI/CD

`jenkins/Jenkinsfile` defines a **declarative pipeline** — a structured description of stages that run sequentially.

```groovy
pipeline {
    agent any   // Run on any available Jenkins agent
    environment {
        DOCKER_REGISTRY_CREDS = credentials('docker-registry-credentials')
        // Jenkins replaces this with the actual credential from the credentials store
        // Creates two variables: DOCKER_REGISTRY_CREDS_USR and DOCKER_REGISTRY_CREDS_PSW
    }
```

#### Stage: `Setup Python Env`

```groovy
sh '''
    python3 -m venv .venv    # Create isolated Python environment
    . .venv/bin/activate     # Activate it (source is . in POSIX sh)
    pip install -r requirements.txt
'''
```
- Creates a fresh virtual environment so the build never uses system Python packages.

#### Stage: `Lint`

```groovy
sh 'flake8 src/ dags/ tests/ --max-line-length=120 --statistics'
```
- `flake8` checks Python style: unused imports, undefined names, style violations.
- `--statistics` shows a count of each error type, useful for seeing the most common issues.

#### Stage: `Unit Tests`

```groovy
sh 'pytest tests/ --cov=src --cov-report=xml:coverage.xml --junitxml=test-results.xml'
```
- `--cov=src` measures which lines of `src/` code are exercised by tests.
- `--cov-report=xml` outputs a coverage XML file for Jenkins to parse and display as a trend graph.
- `--junitxml` outputs test results in JUnit XML format for Jenkins to display pass/fail counts.

#### Stage: `Push to Registry`

```groovy
when {
    anyOf {
        branch 'main'
        branch 'release/*'   // Glob pattern matching any "release/..." branch
    }
}
```
- The push only happens on `main` or release branches. Feature branches run tests but don't push images.

#### Stage: `Deploy`

```groovy
sh 'docker-compose -f docker/docker-compose.yml pull'   // Update to latest image versions
sh 'docker-compose -f docker/docker-compose.yml up -d --remove-orphans --force-recreate'
```
- `--remove-orphans` removes containers for services no longer defined in the compose file.
- `--force-recreate` replaces all containers even if the image hash matches — ensures fresh config.

---

## 10. Tests

All tests use `pytest` with mocked external dependencies. No real OPC API, Oracle DB, or MinIO is needed to run tests.

### `tests/test_p6_client.py`

Uses the `responses` library to intercept HTTP calls and return fake responses.

```python
@responses.activate
def test_get_projects_returns_list(self):
    responses.add(responses.POST, MOCK_CONFIG["token_url"], json=TOKEN_RESPONSE)  # Mock OAuth token
    responses.add(responses.GET, f"{MOCK_CONFIG['base_url']}/projects", json=[...])  # Mock API
    client = OPCClient(config=MOCK_CONFIG)
    result = client.get_projects()
    assert len(result) == 2
```
- `@responses.activate` intercepts all `requests` calls within the test and returns registered mock responses instead of making real HTTP calls.

### `tests/test_transformers.py`

Tests pure math — no mocks needed.

```python
def test_cpi_calculation(self):
    project = self._make_project(pv=1000, ev=800, ac=1000, bac=2000)
    metrics = calculate_project_ev_metrics([project])
    assert metrics[0]["CPI"] == pytest.approx(0.8, rel=1e-4)
```
- `pytest.approx(0.8, rel=1e-4)` allows a relative tolerance of 0.01% when comparing floating-point numbers. Never use `==` for float comparisons.

### `tests/test_loaders.py`

Uses `unittest.mock.patch` to replace `Minio` and `oracledb` with mock objects.

```python
@pytest.fixture
def mock_minio_client():
    with patch("src.loaders.minio_loader.Minio") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        instance.bucket_exists.return_value = True
        yield instance
```
- `patch("src.loaders.minio_loader.Minio")` replaces the `Minio` class in the `minio_loader` module with a mock during the test.
- `MagicMock()` is a magic object that records every call made to it and returns new `MagicMock` instances for attribute access.

---

## 11. Earned Value — Concepts Reference

| Metric | Formula | What it tells you |
|--------|---------|--------------------|
| **PV** | Planned Value | What was planned to cost by now |
| **EV** | Earned Value | Budgeted cost of work actually done |
| **AC** | Actual Cost | Real money spent |
| **BAC** | Budget at Completion | Total approved budget |
| **CPI** | EV / AC | Cost efficiency. 1.0 = on budget, 0.8 = spending 25% more than planned |
| **SPI** | EV / PV | Schedule efficiency. 1.0 = on schedule, 0.8 = 20% behind |
| **CV** | EV − AC | Cost Variance. Negative = over budget |
| **SV** | EV − PV | Schedule Variance. Negative = behind schedule |
| **EAC** | BAC / CPI | Estimate at Completion — final projected cost |
| **VAC** | BAC − EAC | Variance at Completion — projected over/underage |
| **TCPI** | (BAC−EV)/(BAC−AC) | Efficiency needed for remaining work to stay on budget |
| **CV%** | CV / BAC × 100 | Cost variance as % of budget |
| **SV%** | SV / BAC × 100 | Schedule variance as % of budget |

**Interpreting CPI:**
- `CPI > 1.0` — Under budget (getting more than $1 of value per $1 spent)
- `CPI = 1.0` — Exactly on budget
- `CPI < 1.0` — Over budget (e.g., 0.85 = spending $1.18 for every $1 of work)

**Interpreting TCPI:**
- `TCPI > 1.2` — Generally considered unrealistic; the project needs to be re-baselined

---

## 12. Setup & Run Guide

### Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Access to Oracle Primavera Cloud tenant
- Oracle Database (target)
- MinIO server (or use the one in docker-compose)

### Step 1: Configure environment

```bash
cp .env.example .env
# Edit .env with your real credentials:
# - OPC_CLIENT_ID, OPC_CLIENT_SECRET, OPC_BASE_URL, OPC_TOKEN_URL
# - ORACLE_HOST, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD
# - MINIO_ACCESS_KEY, MINIO_SECRET_KEY
```

### Step 2: Set up Oracle Database

```sql
-- Run as a DBA user:
CREATE USER P6_DATA IDENTIFIED BY <password> DEFAULT TABLESPACE USERS;
CREATE USER p6_pipeline_user IDENTIFIED BY <password>;
GRANT CONNECT, RESOURCE TO p6_pipeline_user;

-- Then run the DDL files:
@sql/create_tables.sql
@sql/upsert_procedures.sql
```

### Step 3: Start the stack

```bash
docker-compose -f docker/docker-compose.yml up -d
```

Wait for services to be healthy:
```bash
docker-compose ps   # All should show "(healthy)" or "Up"
```

### Step 4: Configure Airflow

Open http://localhost:8080 (user: `admin`, password: `admin`)

Go to **Admin → Variables** and set:
| Key | Value |
|-----|-------|
| `opc_base_url` | `https://your-tenant.oraclecloud.com/ppmrestapi/v2` |
| `opc_token_url` | `https://idcs-your-tenant.identity.oraclecloud.com/oauth2/v1/token` |
| `opc_client_id` | Your OPC OAuth2 client ID |
| `opc_client_secret` | Your OPC OAuth2 client secret |
| `pipeline_config_path` | `/opt/airflow/config/config.yaml` |
| `eac_method` | `BAC_CPI` |

### Step 5: Run the pipeline

In the Airflow UI:
1. Find `p6_full_pipeline_dag` in the DAG list
2. Toggle it from "Paused" to "Active"
3. Click the ▶ (Trigger DAG) button to run immediately
4. Watch the task graph — each task turns green on success

### Step 6: Verify results

**MinIO Console** (http://localhost:9001):
- `p6-raw/` bucket should contain dated JSON files
- `p6-processed/` bucket should contain Parquet files

**Oracle DB:**
```sql
SELECT PROJECT_NAME, CPI, SPI, EAC, RUN_DATE
FROM P6_DATA.P6_COST_METRICS
ORDER BY RUN_DATE DESC;
```

**Run tests:**
```bash
python -m pytest tests/ -v --cov=src
```

---

*Documentation generated for Oracle OPC P6 Pipeline v1.0.0*
