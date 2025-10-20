import pickle
import datetime
import pandas as pd
import numpy as np
import modules.db_connection as db

# 1) Load env and verify
db.load_env_variables(path="./.env")
assert db.check_env_variables_loaded(), "Env vars not loaded"

# 2) Build connection string and engine, check DB availability
conn_str = db.get_connection_string("defaultdb")
engine = db.get_engine("defaultdb")
db.check_database_available(engine=engine, silent=False)

# 3) Basic metadata/table checks
#    Create a small table and verify existence and non-zero row checks
df_prices = pd.DataFrame(
    {
        "date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "symbol": ["AAPL"] * 5,
        "price": [150.0, 151.2, 150.5, 152.3, 153.1],
        "note": ["ok", "ok", "ok", "ok", "ok"],
    }
)
db.upload_df(
    engine=engine,
    symbol=None,
    df=df_prices,
    table_name="prices",
    categorical_cols=["note"],
    numeric_cols=["price"],
    date_col="date",
    symbol_col="symbol",
    drop_index_cols=False,
    set_index_date_col=True,
    set_index_symbol_col=True,
    update_latest=False,
    alter_table=True,
    dtype_map=None,
    keep_keys_nans=True,
    raise_exception_keys_nans=False,
    raise_exception_overwrite_symbol_col=False,
    silent=False,
)
assert db.check_tables_exist(engine, ["prices"]), "Expected table not found"
assert db.check_nonzero_rows(engine, ["prices"], filter_col="symbol", filter_val="AAPL")

# 4) Create another table using create_table and dtype mapping
custom_map = db.get_default_dtypes_map()
df_metrics = pd.DataFrame(
    {
        "date": pd.date_range("2024-01-01", periods=3, freq="D"),
        "symbol": ["AAPL"] * 3,
        "metric_a": [1.1, 2.2, 3.3],
        "metric_b": ["x", "y", "z"],
    }
)
df_metrics = df_metrics.set_index(["symbol", "date"])
db.create_table(
    engine=engine,
    table_name="metrics",
    categorical_cols=["metric_b"],
    numeric_cols=["metric_a"],
    date_col="date",
    symbol_col="symbol",
    set_index_date_col=True,
    set_index_symbol_col=True,
    dtype_map=custom_map,
)
# Upload after explicit create_table
db.upload_df(
    engine=engine,
    symbol=None,
    df=df_metrics,
    table_name="metrics",
    categorical_cols=["metric_b"],
    numeric_cols=["metric_a"],
    date_col="date",
    symbol_col="symbol",
    set_index_date_col=True,
    set_index_symbol_col=True,
    update_latest=False,
    alter_table=False,
    raise_exception_overwrite_symbol_col=False,
    silent=False,
)

# 5) Union of columns across tables and schema utilities
all_cols = db.get_union_all_columns(engine, tables=["prices", "metrics"])
_ = db.sql_column_strings(df_metrics, exclude_cols=["date", "symbol"], dtype_map=custom_map)

# 6) Update latest row behavior
df_prices_update = df_prices.copy()
# Modify last point to demonstrate update_latest=True
df_prices_update.loc[df_prices_update["date"].idxmax(), "price"] = 154.0
db.upload_df(
    engine=engine,
    symbol=None,
    df=df_prices_update,
    table_name="prices",
    categorical_cols=["note"],
    numeric_cols=["price"],
    date_col="date",
    symbol_col="symbol",
    set_index_date_col=True,
    set_index_symbol_col=True,
    update_latest=True,
    alter_table=False,
    raise_exception_overwrite_symbol_col=False,
    silent=False,
)

# 7) Latest date query for a symbol
latest_dt = db.get_latest_date_symbol(
    engine=engine,
    table_name="prices",
    symbol="AAPL",
    date_col="date",
    symbol_col="symbol",
    raise_exception=False,
)

# 8) Fetch everything for a symbol across all tables
all_data = db.get_all_symbol_data(engine, symbol="AAPL", date_col="date", symbol_col="symbol", drop_symbol_col=False, drop_id_col=True, silent=False)

# 9) Upload and fetch a dict row (overwrites existing)
example_dict = {"field_a": 123, "field_b": 4.56, "field_c": "hello"}
db.upload_dict(
    engine=engine,
    symbol="AAPL",
    d=example_dict,
    table_name="meta_single",
    symbol_col="symbol",
    alter_table=True,
    dtype_map=None,
    silent=False,
)

# 10) ORM dynamic class from dict and helper (demonstrated via upload_dict + helper inside)
#    Direct helper usage (no overwrite guarantees) for demo:
db.helper_upload_dict(engine=engine, d={"field_a": 7, "field_c": "world"}, table_name="meta_helper", symbol_col=None)

# 11) Store and retrieve an arbitrary Python object as a pickle
obj = {"when": datetime.datetime.now(datetime.UTC), "arr": np.array([1, 2, 3])}
db.upload_object(
    engine=engine,
    obj=obj,
    table_name="object_store",
    date=datetime.datetime.now(datetime.UTC),
    symbol="AAPL",
    date_col="date",
    symbol_col="symbol",
)
restored = db.get_object(
    engine=engine,
    table_name="object_store",
    symbol="AAPL",
    date_col="date",
    symbol_col="symbol",
)

# 12) Simple SELECT helpers
df_selected = db.get_df_symbol_request(engine, request="SELECT * FROM `prices`", symbol="AAPL")
df_symbols = db.get_df_symbols_data(engine, symbols=["AAPL"], table_name="prices", symbol_col="symbol")
df_one = db.get_symbol_data(engine, symbol="AAPL", table_name="prices", date_col="date", symbol_col="symbol", drop_symbol_col=True)

# 13) Table existence re-check and get existing rows utility
assert db.check_tables_exist(engine, ["prices", "metrics"])
# For get_existing_rows, compare subset data
existing_df = df_one.reset_index().head(2)
db_df_subset = df_selected.head(2)
db.get_existing_rows(existing_df, db_df_subset)

# 14) Clean up engine
engine.dispose(close=True)