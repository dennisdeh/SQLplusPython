# SQL+Python
A collection of utils to effortlessly and reliably interact between Python and your SQL database — purpose‑built for time‑series workflows.
This package streamlines reading, writing, and maintaining SQL tables using familiar Python structures like pandas DataFrames, dictionaries, and other Python objects. It’s user-friendly, strict about data consistency, and tuned for time-series data pipelines.
Intended for MariaDB/MySQL via SQLAlchemy - pre-configured docker environment included.

### Design Principles
- Predictable schemas: deterministic dtype mapping and optional auto-alter
- Strong safeguards: explicit checks, clear assertions, and helpful errors
- Pandas-friendly: minimal friction between DataFrame types and SQL
- Maintainability: composable helpers for upload, fetch, and schema operations

## Key Features
- Focus on time-series data
    - Append-only patterns with optional “update latest” logic
    - Automatic handling of date/symbol index columns
    - Fast retrieval by symbol and date with optional indexing
    - Utilities to query by symbol(s), fetch latest dates, and union columns across tables

- Works with your data structures
    - Upload pandas DataFrames with automatic dtype mapping
    - Store/retrieve dictionaries as rows
    - Persist arbitrary Python objects via pickling (e.g., models, configs)
    - Minimal boilerplate for table creation and updates
    - Smart dtype defaults for SQL schema generation

- Safety and consistency checks
    - NaN/Inf coercion and validation for key columns
    - Duplicate/consistency guards when updating recent rows
    - Table introspection: add missing columns automatically (optional)
    - Environment validation and connection checks

### Typical Use Cases
- Maintain historical time-series data on a symbol-level (prices, indicators, metrics)
- Incrementally update tables from pandas pipelines
- Keep per-symbol metadata and snapshots
- Store models or transforms in SQL as versioned pickles

## Installation
- Python: 3.12+
- Installed packages: sqlalchemy, pandas etc.
- Optional: Docker to use the docker compose environment that is set up to work wiht the utils - batteries included

Create/activate your virtualenv, then install your project’s dependencies as usual with pip inside the virtualenv.
Run `docker compose up` in the project root to start the MariaDB container with the default environment variables.

### Environment Variables
- Standard DB credentials (e.g., user, password, host, port, database) loaded from your .env
- A default setup for Docker is included
- Built-in validation for host configuration; safe fallbacks if misconfigured


## Quick Start
The main functions are listed below.

- `upload_df()`: DataFrame uploads
    - Auto-creates tables with sensible column types (numeric as DOUBLE, categoricals as TEXT, datetimes as DATETIME) using `create_table()`
    - Optional table alteration to add newly appearing DataFrame columns
    - Optional enforcement of non-null date/symbol keys with configurable behavior
    - When uploading time-series data and using “update latest,” only the newest overlapping row(s) are replaced; older history remains intact

- `upload_dict()`: Dictionary uploads
    - Keys become columns; can define a symbol column for idempotent updates

- `upload_object()`: Object uploads
    - Serialize Python objects via pickle and store them in LONGBLOB columns

- Retrieval helpers
    - `get_symbol_data()` or `get_df_symbols_data()`: Get time-series for a single symbol or multiple symbols
    - `get_all_symbol_data()`: Fetch all tables for a symbol in one go
    - `get_existing_rows()`: Inspect availability (tables exist, rows > 0), and compute the union of columns across tables



