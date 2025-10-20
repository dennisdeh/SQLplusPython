import datetime
import unittest
from typing import Union
import sqlpluspython.db_connection as db
import pandas as pd
import numpy as np
import itertools
import sqlpluspython.utils.lists as lists
import sqlalchemy
from sqlpluspython.testing.aa_cleaner.clean_test_database import (
    clean_test_database,
    reset_test_tables,
)
from sqlpluspython.utils.generate_test_data import gdf

test_helpers_only = False
skip_upload_df = False


class DatabaseFunctionsHelpers(unittest.TestCase):
    """
    Tests of functions helping with the database interaction,
    but do not directly interact with the database
    """

    def test_load_env_variables(self):
        path_env = "modules/testing/data/.env"
        self.assertIsNone(db.load_env_variables(path=path_env))
        self.assertRaises(
            FileNotFoundError, db.load_env_variables, **{"path": "some/false/path"}
        )

    def test_sql_column_string(self):
        # 1: dataframe, no exclude
        df = gdf()
        out = db.sql_column_strings(inpt=df, exclude_cols=None, dtype_map=None)
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(df.columns))
        for e in out:
            if e.find("datetime") > -1:
                self.assertGreater(e.find("DATETIME"), 0)
            elif e.find("float") > -1:
                self.assertGreater(e.find("DOUBLE"), 0)
            elif e.find("str") > -1:
                self.assertGreater(e.find("TEXT"), 0)
            else:
                continue
        # 2: dataframe, exclude
        df = gdf()
        out = db.sql_column_strings(inpt=df, exclude_cols=["int"], dtype_map=None)
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(df.columns) - 1)
        for e in out:
            if e.find("datetime") > -1:
                self.assertGreater(e.find("DATETIME"), 0)
            elif e.find("float") > -1:
                self.assertGreater(e.find("DOUBLE"), 0)
            elif e.find("str") > -1:
                self.assertGreater(e.find("TEXT"), 0)
            else:
                continue
        # 3: dict, no exclude
        d = {
            "A": 1,
            "B": 10.01,
            "C": None,
            "D": [21, 1, 3],
            "E": "string sdasd",
            "F": datetime.datetime(1900, 1, 3),
            "G": True,
        }
        out = db.sql_column_strings(inpt=d, exclude_cols=None, dtype_map=None)
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(d))
        for e in out:
            if e.find("`B`") > -1:
                self.assertGreater(e.find("DOUBLE"), 0)
            elif e.find("`C`") > -1 or e.find("`D`") > -1 or e.find("`E`") > -1:
                self.assertGreater(e.find("TEXT"), 0)
            elif e.find("`F`") > -1:
                self.assertGreater(e.find("DATETIME"), 0)
            elif e.find("`G`") > -1:
                self.assertGreater(e.find("BOOLEAN"), 0)
            else:
                continue
        # 4: dict, exclude
        d = {
            "A": 1,
            "B": 10.01,
            "C": None,
            "D": [21, 1, 3],
            "E": "string sdasd",
            "F": datetime.datetime(1900, 1, 3),
            "G": True,
        }
        out = db.sql_column_strings(inpt=d, exclude_cols=["C"], dtype_map=None)
        self.assertIsInstance(out, list)
        self.assertEqual(len(out), len(d) - 1)
        for e in out:
            self.assertEqual(e.find("`C`"), -1)
            if e.find("`B`") > -1:
                self.assertGreater(e.find("DOUBLE"), 0)
            elif e.find("`D`") > -1 or e.find("`E`") > -1:
                self.assertGreater(e.find("TEXT"), 0)
            elif e.find("`F`") > -1:
                self.assertGreater(e.find("DATETIME"), 0)
            elif e.find("`G`") > -1:
                self.assertGreater(e.find("BOOLEAN"), 0)
            else:
                continue


@unittest.skipIf(test_helpers_only, "testing helper functions only")
class DatabaseFunctionsReading(unittest.TestCase):
    """
    Tests of functions for interacting with the database MariaDB
    SQL backend for reading tables
    """

    @classmethod
    def setUpClass(cls):
        """
        Setup of SQL engine for the database
        """
        # initialise
        cls.path_db_env = "./.env"
        db.load_env_variables(path=cls.path_db_env)
        cls.engine = db.get_engine("testing")
        cls.reset_db = True

        # always reset the testing database before running tests
        reset_test_tables(engine=cls.engine, confirm=True)

    @classmethod
    def tearDownClass(cls):
        """
        Reset testing database and disposal of the SQL engine
        """
        if cls.reset_db:
            reset_test_tables(engine=cls.engine, confirm=True)
        cls.engine.dispose(close=True)

    def test_read_dates_symbols(self):
        # 1: upload df with many different dates
        n = 1000
        df = gdf(
            n=n, n_copies=5, seed=1, date_start="1900-01-01", date_end="2020-12-31"
        )
        # upload
        db.upload_df(
            engine=self.engine,
            symbol=None,
            df=df,
            table_name="test_table_reading0",
            categorical_cols=None,
            numeric_cols=None,
            date_col="datetime",
            symbol_col="str",
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=True,
            columns_to_drop=None,
            dtype_map=None,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks symbols that we know are there
        for sym in ["A", "B", "C"]:
            out = db.get_latest_date_symbol(
                engine=self.engine,
                table_name="test_table_reading0",
                date_col="datetime",
                symbol_col="str",
                symbol=sym,
            )
            self.assertGreaterEqual(out, datetime.datetime(1900, 1, 1))
            self.assertLessEqual(out, datetime.datetime(2020, 12, 31))
        # checks for symbols we know are not there
        out = db.get_latest_date_symbol(
            engine=self.engine,
            table_name="test_table_reading0",
            date_col="datetime",
            symbol_col="str",
            symbol="DOES_NOT_EXIST",
        )
        self.assertIsNone(out)


@unittest.skipIf(test_helpers_only, "testing helper functions only")
class DatabaseFunctionsCreate(unittest.TestCase):
    """
    Tests of functions for interacting with the database MariaDB
    SQL backend for creating and manipulating tables
    """

    @classmethod
    def setUpClass(cls):
        """
        Setup of SQL engine for database
        """
        # initialise
        cls.path_db_env = "./.env"
        db.load_env_variables(path=cls.path_db_env)
        cls.engine = db.get_engine("testing")
        cls.reset_db = True

        # always reset testing database before running tests
        reset_test_tables(engine=cls.engine, confirm=True)

    @classmethod
    def tearDownClass(cls):
        """
        Reset testing database and disposal of the SQL engine
        """
        if cls.reset_db:
            reset_test_tables(engine=cls.engine, confirm=True)
        cls.engine.dispose(close=True)

    def check_df_equivalence(
        self, df1: pd.DataFrame, df2: pd.DataFrame, cat_col_to_exclude: Union[str, None]
    ):
        """
        This method checks that two dataframes are equivalent,
        meaning that the numerical datatype is not verified,
        but all values are checked if they are equal.
        For other datatypes exact equality is checked.
        A column can be excluded from these checks
        """
        # 1: test that numeric columns are equivalent (up to dtype)
        numeric_cols = df1.select_dtypes(include="number").columns.tolist()
        self.assertIsNone(
            pd.testing.assert_frame_equal(
                df1[numeric_cols],
                df2[numeric_cols],
                check_dtype=False,  # dtypes changes with encoding
            )
        )
        # 2: check non-numeric columns that are not the symbol column
        if cat_col_to_exclude is not None:
            non_numeric_cols = lists.difference(
                df1.columns, lists.union(numeric_cols, [cat_col_to_exclude])
            )
        else:
            non_numeric_cols = lists.difference(df1.columns, numeric_cols)
        self.assertIsNone(
            pd.testing.assert_frame_equal(df1[non_numeric_cols], df2[non_numeric_cols])
        )

    def test_create_table(self):
        """
        Test that all sorts of different tables can be created using the
        create_table function.
        """
        # 0: initialise
        # 0.1: generate a testing dataframe
        df = gdf(n=1000, n_copies=4, seed=1)
        # 0.2: lists to iterate over
        list_categorical_cols = [None, ["str_1", "str_2", "float_3"]]
        list_numeric_cols = [None, ["float", "int", "int_3"]]
        list_date_col = [None, "datetime"]
        list_symbol_col = [None, "str"]
        list_set_index_date_col = [False, True]
        list_set_index_symbol_col = [False, True]
        list_df = [None, df]
        list_dtype_map = [
            None,
            {
                "int64": "DOUBLE",
                "float64": "DOUBLE",
                "object": "TEXT",
                "datetime64[ns]": "DATETIME",
                "bool": "DOUBLE",
            },
        ]

        # 1: Run all tests verifying that create_table() works
        n = 0
        for element in itertools.product(
            list_categorical_cols,
            list_numeric_cols,
            list_date_col,
            list_symbol_col,
            list_set_index_date_col,
            list_set_index_symbol_col,
            list_df,
            list_dtype_map,
        ):
            with self.subTest(element=element):
                # Initialise
                categorical_cols = element[0]
                numeric_cols = element[1]
                date_col = element[2]
                symbol_col = element[3]
                set_index_date_col = element[4]
                set_index_symbol_col = element[5]
                df = element[6]
                dtype_map = element[7]

                if categorical_cols is None and numeric_cols is None and df is None:
                    continue
                # check that no outputs are created
                self.assertIsNone(
                    db.create_table(
                        engine=self.engine,
                        table_name=f"test_create_table{n}",
                        categorical_cols=categorical_cols,
                        numeric_cols=numeric_cols,
                        date_col=date_col,
                        symbol_col=symbol_col,
                        set_index_date_col=set_index_date_col,
                        set_index_symbol_col=set_index_symbol_col,
                        df=df,
                        dtype_map=dtype_map,
                    )
                )
                # check that the table is indeed exists and is empty
                self.assertFalse(
                    db.check_nonzero_rows(
                        engine=self.engine,
                        mandatory_tables=[f"test_create_table{n}"],
                        filter_col=None,
                        filter_val=None,
                    )
                )
                # check index columns: get query
                with self.engine.connect() as connection:
                    index_query = f"SHOW INDEX FROM test_create_table{n}"
                    # execute the query and get the result
                    result = connection.execute(sqlalchemy.text(index_query))
                    index_info = result.fetchall()
                # compare with expected number of index columns
                exp_index_columns = int(
                    set_index_date_col and (date_col is not None)
                ) + int(set_index_symbol_col and (symbol_col is not None))
                self.assertEqual(len(index_info), exp_index_columns)

                # prepare for next iteration
                n += 1

        # 2: test exceptions
        # 2.1: inputs
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": None,
                "table_name": "test_table_zz",
                "categorical_cols": ["str_1", "str_2", "float"],
                "numeric_cols": ["float_1", "int", "int_3"],
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": df,
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": None,
                "categorical_cols": ["str_1", "str_2", "float"],
                "numeric_cols": ["float_1", "int", "int_3"],
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": df,
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": dict(),
                "numeric_cols": ["float_1", "int", "int_3"],
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": df,
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": None,
                "numeric_cols": dict(),
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": df,
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": [],
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": df,
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": None,
                "symbol_col": [],
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": df,
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": [],
                "set_index_symbol_col": True,
                "df": df,
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": [],
                "df": df,
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": pd.Series(),
                "dtype_map": None,
            },
        )
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": None,
                "dtype_map": [],
            },
        )
        # 2.2 test that there are no overlaps in numerical and categorical column names
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": ["str_1", "str_2", "float"],
                "numeric_cols": ["float", "int", "int_3"],
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": df,
                "dtype_map": None,
            },
        )
        # 2.3: test if categorical_cols is None and numeric_cols is None and df is None
        self.assertRaises(
            AssertionError,
            db.create_table,
            **{
                "engine": self.engine,
                "table_name": "test_table_zz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": "datetime",
                "symbol_col": "str_1",
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "df": None,
                "dtype_map": None,
            },
        )

        # 3: reset testing database
        if self.reset_db:
            reset_test_tables(engine=self.engine, confirm=True)

    def test_upload_dict(self):
        """
        Tests for uploading a dictionary to the database
        """
        # 0: initialise test variables
        d0 = {
            "A": 1,
            "B": 10.01,
            "C": None,
            "D": False,
            "E": "string sdasd",
            "F": datetime.datetime(1900, 1, 3),
            "G": True,
        }
        d1 = {
            "A": 2,
            "B": 20.01,
            "C": None,
            "D": True,
            "E": "string D#D",
            "F": datetime.datetime(2000, 1, 3),
            "G": False,
        }
        d2 = {
            "A": 3,
            "B": 30.05,
            "C": "",
            "D": True,
            "E": "string KJ!Ss",
            "F": datetime.datetime(2002, 1, 3),
            "G": False,
            "extra": "something",
        }

        # Part A: upload with a non-existing symbol_col
        # 1: upload first dict to the database (non-existing symbol_col)
        db.upload_dict(
            engine=self.engine,
            symbol="test0",
            d=d0,
            table_name="test_table_dict",
            symbol_col="symbol",
            alter_table=False,
            dtype_map=None,
            silent=False,
        )
        # read back the dict that was uploaded
        with self.engine.connect() as connection:
            # read from the database
            df_read = pd.read_sql("test_table_dict", connection)
            # expected shape and values (symbol and id columns are added)
            self.assertEqual(df_read.shape[0], 1)
            self.assertEqual(df_read.shape[1], len(d0.keys()) + 2)
            # id (auto-incremented)
            self.assertEqual(df_read["id"].iloc[0], 1)
            # A known column (symbol column)
            self.assertEqual(df_read["symbol"].iloc[0], "test0")

        # 2: upload first dict to the database (non-existing symbol_col) again
        db.upload_dict(
            engine=self.engine,
            symbol="test0",
            d=d0,
            table_name="test_table_dict",
            symbol_col="symbol",
            alter_table=False,
            dtype_map=None,
            silent=False,
        )
        # read back the dict that was uploaded
        with self.engine.connect() as connection:
            # read from the database
            df_read = pd.read_sql("test_table_dict", connection)
            # expected shape and values (symbol and id columns are added)
            self.assertEqual(df_read.shape[0], 1)
            self.assertEqual(df_read.shape[1], len(d0.keys()) + 2)
            # id (auto-incremented)
            self.assertEqual(df_read["id"].iloc[0], 2)
            # A known column (symbol column)
            self.assertEqual(df_read["symbol"].iloc[0], "test0")

        # 3: Add a new dictionary to the database (same columns, different symbol)
        db.upload_dict(
            engine=self.engine,
            symbol="test1",
            d=d1,
            table_name="test_table_dict",
            symbol_col="symbol",
            alter_table=False,
            dtype_map=None,
            silent=False,
        )
        # read back the dict that was uploaded
        with self.engine.connect() as connection:
            # read from the database
            df_read = pd.read_sql("test_table_dict", connection)
            # expected shape and values (symbol and id columns are added)
            self.assertEqual(df_read.shape[0], 2)
            self.assertEqual(df_read.shape[1], len(d1.keys()) + 2)
            # id (auto-incremented)
            self.assertEqual(df_read["id"].iloc[0], 2)
            self.assertEqual(df_read["id"].iloc[1], 3)
            # A known column (symbol column)
            self.assertEqual(df_read["symbol"].iloc[0], "test0")
            self.assertEqual(df_read["symbol"].iloc[1], "test1")
        # 4: Add the new dictionary to the database again
        db.upload_dict(
            engine=self.engine,
            symbol="test1",
            d=d1,
            table_name="test_table_dict",
            symbol_col="symbol",
            alter_table=False,
            dtype_map=None,
            silent=False,
        )
        # read back the dict that was uploaded
        with self.engine.connect() as connection:
            # read from the database
            df_read = pd.read_sql("test_table_dict", connection)
            # expected shape and values (symbol and id columns are added)
            self.assertEqual(df_read.shape[0], 2)
            self.assertEqual(df_read.shape[1], len(d1.keys()) + 2)
            # id (auto-incremented)
            self.assertEqual(df_read["id"].iloc[0], 2)
            self.assertEqual(df_read["id"].iloc[1], 4)
            # A known column (symbol column)
            self.assertEqual(df_read["symbol"].iloc[0], "test0")
            self.assertEqual(df_read["symbol"].iloc[1], "test1")
        # 5: Add a new dictionary to the database, which has one more key
        db.upload_dict(
            engine=self.engine,
            symbol="test2",
            d=d2,
            table_name="test_table_dict",
            symbol_col="symbol",
            alter_table=True,
            dtype_map=None,
            silent=False,
        )
        # read back the dict that was uploaded
        with self.engine.connect() as connection:
            # read from the database
            df_read = pd.read_sql("test_table_dict", connection)
            # expected shape and values (symbol and id columns are added)
            self.assertEqual(df_read.shape[0], 3)
            self.assertEqual(df_read.shape[1], len(d2.keys()) + 2)
            # id (auto-incremented)
            self.assertEqual(df_read["id"].iloc[0], 2)
            self.assertEqual(df_read["id"].iloc[1], 4)
            self.assertEqual(df_read["id"].iloc[2], 5)
            # A known column (symbol column)
            self.assertEqual(df_read["symbol"].iloc[0], "test0")
            self.assertEqual(df_read["symbol"].iloc[1], "test1")
            self.assertEqual(df_read["symbol"].iloc[2], "test2")
            # check the extra new column
            self.assertIsNone(df_read["extra"].iloc[0])
            self.assertIsNone(df_read["extra"].iloc[1])
            self.assertEqual(df_read["extra"].iloc[2], "something")
        # 6: Add the new dictionary to the database again
        db.upload_dict(
            engine=self.engine,
            symbol="test2",
            d=d2,
            table_name="test_table_dict",
            symbol_col="symbol",
            alter_table=True,
            dtype_map=None,
            silent=False,
        )
        # read back the dict that was uploaded
        with self.engine.connect() as connection:
            # read from the database
            df_read = pd.read_sql("test_table_dict", connection)
            # expected shape and values (symbol and id columns are added)
            self.assertEqual(df_read.shape[0], 3)
            self.assertEqual(df_read.shape[1], len(d2.keys()) + 2)
            # id (auto-incremented)
            self.assertEqual(df_read["id"].iloc[0], 2)
            self.assertEqual(df_read["id"].iloc[1], 4)
            self.assertEqual(df_read["id"].iloc[2], 6)
            # A known column (symbol column)
            self.assertEqual(df_read["symbol"].iloc[0], "test0")
            self.assertEqual(df_read["symbol"].iloc[1], "test1")
            self.assertEqual(df_read["symbol"].iloc[2], "test2")
            # check the extra new column
            self.assertIsNone(df_read["extra"].iloc[0])
            self.assertIsNone(df_read["extra"].iloc[1])
            self.assertEqual(df_read["extra"].iloc[2], "something")

        # Part B: upload with an existing symbol_col
        for d in [d0, d1, d2]:
            db.upload_dict(
                engine=self.engine,
                symbol=None,
                d=d,
                table_name="test_table_dictB",
                symbol_col="A",
                alter_table=True,
                dtype_map=None,
                silent=False,
            )
        # read back the dict that was uploaded
        with self.engine.connect() as connection:
            # read from the database
            df_read = pd.read_sql("test_table_dictB", connection)
            # expected shape and values (id column is added)
            self.assertEqual(df_read.shape[0], 3)
            self.assertEqual(df_read.shape[1], len(d2.keys()) + 1)
            # id (auto-incremented)
            self.assertEqual(df_read["id"].iloc[0], 1)
            self.assertEqual(df_read["id"].iloc[1], 2)
            self.assertEqual(df_read["id"].iloc[2], 3)
            # a known column (A column)
            self.assertEqual(df_read["A"].iloc[0], 1)
            self.assertEqual(df_read["A"].iloc[1], 2)
            self.assertEqual(df_read["A"].iloc[2], 3)
            # check the extra new column
            self.assertIsNone(df_read["extra"].iloc[0])
            self.assertIsNone(df_read["extra"].iloc[1])
            self.assertEqual(df_read["extra"].iloc[2], "something")

        # Part C: upload with no symbol_col
        for d in [d0, d1, d2]:
            db.upload_dict(
                engine=self.engine,
                symbol=None,
                d=d,
                table_name="test_table_dictC",
                symbol_col=None,
                alter_table=True,
                dtype_map=None,
                silent=False,
            )
        # read back the dict that was uploaded
        with self.engine.connect() as connection:
            # read from the database
            df_read = pd.read_sql("test_table_dictC", connection)
            # expected shape and values (id column is added)
            self.assertEqual(df_read.shape[0], 3)
            self.assertEqual(df_read.shape[1], len(d2.keys()) + 1)
            # id (auto-incremented)
            self.assertEqual(df_read["id"].iloc[0], 1)
            self.assertEqual(df_read["id"].iloc[1], 2)
            self.assertEqual(df_read["id"].iloc[2], 3)
            # a known column (A column)
            self.assertEqual(df_read["A"].iloc[0], 1)
            self.assertEqual(df_read["A"].iloc[1], 2)
            self.assertEqual(df_read["A"].iloc[2], 3)
            # check the extra new column
            self.assertIsNone(df_read["extra"].iloc[0])
            self.assertIsNone(df_read["extra"].iloc[1])
            self.assertEqual(df_read["extra"].iloc[2], "something")

        # Part D: Special dictionaries
        # None
        db.upload_dict(
            engine=self.engine,
            symbol=None,
            d=None,
            table_name="test_table_dictD",
            symbol_col="A",
            alter_table=True,
            dtype_map=None,
            silent=False,
        )
        # Empty dict
        db.upload_dict(
            engine=self.engine,
            symbol=None,
            d={},
            table_name="test_table_dictD",
            symbol_col="A",
            alter_table=True,
            dtype_map=None,
            silent=False,
        )

        # Part E: expected exceptions
        # symbol_col given, but no symbol
        self.assertRaises(
            ValueError,
            db.upload_dict,
            **{
                "engine": self.engine,
                "symbol": None,
                "d": d0,
                "table_name": "test_table_dictE",
                "symbol_col": "str",
                "alter_table": True,
                "dtype_map": None,
                "silent": False,
            },
        )
        # symbol given, but no symbol_col
        self.assertRaises(
            ValueError,
            db.upload_dict,
            **{
                "engine": self.engine,
                "symbol": "SOMETHING",
                "d": d0,
                "table_name": "test_table_dictE",
                "symbol_col": None,
                "alter_table": True,
                "dtype_map": None,
                "silent": False,
            },
        )

    @unittest.skipIf(skip_upload_df, "skipping all tests involving upload_df")
    def test_upload_df_creating_new_tables(self):
        """
        Tests involving writing once to the database.
        """
        # 1: upload with symbol, symbol_col, date_col, no indices, symbol (to overwrite)
        #   drop index_cols, dtype_map, !raise_exception_overwrite_symbol_col
        n = 501
        df = gdf(n=n, n_copies=5, seed=1)
        test_table = "test_tableA1"
        symbol = "A"
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = {
            "int64": "BIGINT",
            "Int64": "BIGINT",
            "float64": "DOUBLE",
            "Float64": "DOUBLE",
            "object": "TEXT",
            "datetime64[ns]": "DATETIME",
            "bool": "BOOLEAN",
        }
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=False,
            set_index_symbol_col=False,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=False,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], n)
            self.assertEqual(df_read.shape[1], 20)
            self.assertEqual((df_read[symbol_col] == symbol).sum(), n)
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )

        # 2: upload with !symbol, symbol_col, date_col, no indices, drop index_cols, dtype_map
        n = 502
        df = gdf(n=n, n_copies=5, seed=2)
        test_table = "test_tableA2"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = {
            "int64": "BIGINT",
            "Int64": "BIGINT",
            "float64": "DOUBLE",
            "Float64": "DOUBLE",
            "object": "TEXT",
            "datetime64[ns]": "DATETIME",
            "bool": "BOOLEAN",
        }
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=False,
            set_index_symbol_col=False,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], n)
            self.assertEqual(df_read.shape[1], 20)
            self.assertLess((df_read[symbol_col] == symbol).sum(), n)
            self.assertLess(df_read[symbol_col].isna().sum(), n)
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )

        # 3: upload with !symbol, symbol_col, date_col, no indices, !drop index_cols, dtype_map
        n = 503
        df = gdf(n=n, n_copies=5, seed=3)
        test_table = "test_tableA3"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = {
            "int64": "BIGINT",
            "Int64": "BIGINT",
            "float64": "DOUBLE",
            "Float64": "DOUBLE",
            "object": "TEXT",
            "datetime64[ns]": "DATETIME",
            "bool": "BOOLEAN",
        }
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=False,
            set_index_date_col=False,
            set_index_symbol_col=False,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], n)
            self.assertEqual(df_read.shape[1], 21)
            self.assertLess((df_read[symbol_col] == symbol).sum(), n)
            self.assertLess(df_read[symbol_col].isna().sum(), n)
            # drop "index" column (old index column before uploading) to compare
            self.assertTrue("index" in df_read.columns)
            df_read = df_read.drop(columns=["index"])
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )

        # 4: upload with !symbol, symbol_col, date_col, !no indices, !drop index_cols, dtype_map
        n = 504
        df = gdf(n=n, n_copies=5, seed=4)
        test_table = "test_tableA4"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = {
            "int64": "BIGINT",
            "Int64": "BIGINT",
            "float64": "DOUBLE",
            "Float64": "DOUBLE",
            "object": "TEXT",
            "datetime64[ns]": "DATETIME",
            "bool": "BOOLEAN",
        }
        # a) set date_col as index
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=f"{test_table}a",
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=False,
            set_index_date_col=True,
            set_index_symbol_col=False,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # b) set symbol_col as index
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=f"{test_table}b",
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=False,
            set_index_date_col=False,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # c) set date_col and symbol_col as indices
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=f"{test_table}c",
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=False,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            for s in ["a", "b", "c"]:
                df_read = pd.read_sql(f"{test_table}{s}", connection)
                # expected shape
                self.assertEqual(df_read.shape[0], n)
                self.assertEqual(df_read.shape[1], 21)
                self.assertLess((df_read[symbol_col] == symbol).sum(), n)
                self.assertLess(df_read[symbol_col].isna().sum(), n)
                # drop "index" column (old index column before uploading) to compare
                self.assertTrue("index" in df_read.columns)
                df_read = df_read.drop(columns=["index"])
                # check equivalence of dataframes
                self.check_df_equivalence(
                    df1=df_read, df2=df, cat_col_to_exclude=symbol_col
                )
                # check index columns: get query
                index_query = f"SHOW INDEX FROM {test_table}{s}"
                # execute the query and get the result
                result = connection.execute(sqlalchemy.text(index_query))
                index_info = result.fetchall()
                if s == "a":
                    self.assertEqual(len(index_info), 1)
                    self.assertEqual(index_info[0][1], 1)
                    self.assertEqual(index_info[0][4], "datetime")
                elif s == "b":
                    self.assertEqual(len(index_info), 1)
                    self.assertEqual(index_info[0][1], 1)
                    self.assertEqual(index_info[0][4], "str")
                else:
                    self.assertEqual(len(index_info), 2)
                    self.assertEqual(index_info[0][1], 1)
                    self.assertEqual(index_info[0][4], "datetime")
                    self.assertEqual(index_info[1][1], 1)
                    self.assertEqual(index_info[1][4], "str")

        # 5: upload with !symbol, symbol_col, date_col, no indices, !drop index_cols, !dtype_map
        df = gdf(n=n, n_copies=5, seed=5)
        test_table = "test_tableA5"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=False,
            set_index_symbol_col=False,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], n)
            self.assertEqual(df_read.shape[1], 20)
            self.assertLess((df_read[symbol_col] == symbol).sum(), n)
            self.assertLess(df_read[symbol_col].isna().sum(), n)
            # assert "index" column (old index column before uploading) is not present
            self.assertFalse("index" in df_read.columns)
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )

        # 6: upload with !symbol, !symbol_col, date_col, indices, !drop index_cols, !dtype_map
        n = 506
        df = gdf(n=n, n_copies=5, seed=6)
        test_table = "test_tableA6"
        symbol = None
        date_col = "datetime"
        symbol_col = None
        dtype_map = None
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], n)
            self.assertEqual(df_read.shape[1], 20)
            # assert "index" column (old index column before uploading) is not present
            self.assertFalse("index" in df_read.columns)
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )
            # check index columns: get query
            index_query = f"SHOW INDEX FROM {test_table}"
            # execute the query and get the result
            result = connection.execute(sqlalchemy.text(index_query))
            index_info = result.fetchall()
            self.assertEqual(len(index_info), 1)
            self.assertEqual(index_info[0][1], 1)
            self.assertEqual(index_info[0][4], "datetime")

        # 7: upload with !symbol, !symbol_col, date_col, indices, !drop index_cols, !dtype_map
        n = 507
        df = gdf(n=n, n_copies=5, seed=7)
        test_table = "test_tableA7"
        symbol = None
        date_col = None
        symbol_col = "str"
        dtype_map = None
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], n)
            self.assertEqual(df_read.shape[1], 20)
            # assert "index" column (old index column before uploading) is not present
            self.assertFalse("index" in df_read.columns)
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )
            # check index columns: get query
            index_query = f"SHOW INDEX FROM {test_table}"
            # execute the query and get the result
            result = connection.execute(sqlalchemy.text(index_query))
            index_info = result.fetchall()
            self.assertEqual(len(index_info), 1)
            self.assertEqual(index_info[0][1], 1)
            self.assertEqual(index_info[0][4], "str")

        # 8: upload with !symbol, !symbol_col, date_col, indices, !drop index_cols, !dtype_map
        n = 508
        df = gdf(n=n, n_copies=5, seed=8)
        test_table = "test_tableA8"
        symbol = None
        date_col = None
        symbol_col = None
        dtype_map = None
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], n)
            self.assertEqual(df_read.shape[1], 20)
            # assert "index" column (old index column before uploading) is not present
            self.assertFalse("index" in df_read.columns)
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )
            # check index columns: get query
            index_query = f"SHOW INDEX FROM {test_table}"
            # execute the query and get the result
            result = connection.execute(sqlalchemy.text(index_query))
            index_info = result.fetchall()
            self.assertEqual(len(index_info), 0)

        # 9: upload with !symbol, !symbol_col, date_col, indices, !drop index_cols, !dtype_map, !keep_nan
        n = 509
        # 9a: date_col
        df = gdf(n=n, n_copies=5, seed=8)
        test_table = "test_tableA9a"
        symbol = None
        date_col = "datetime"
        symbol_col = None
        dtype_map = None
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df[~df[date_col].isna()]))
            self.assertEqual(df_read.shape[1], 20)
        # 9b: symbol_col
        test_table = "test_tableA9b"
        date_col = None
        symbol_col = "str"
        dtype_map = None
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # get expected dataframe to compare with
            df = df[~df[symbol_col].isna()]
            df = df.reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df))
            self.assertEqual(df_read.shape[1], 20)
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )
        # 9c: date_col + symbol_col
        test_table = "test_tableA9c"
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # get expected dataframe to compare with
            df = df[~df[date_col].isna() & ~df[symbol_col].isna()]
            df = df.reset_index(drop=True)
            # check expected shape
            self.assertEqual(df_read.shape[0], len(df))
            self.assertEqual(df_read.shape[1], 20)
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )

    @unittest.skipIf(skip_upload_df, "skipping all tests involving upload_df")
    def test_upload_df_appending_existing_tables(self):
        """
        Tests involving writing twice or more to the database.
        """

        # %% 1: update existing table, keep_nans
        n = 501
        # create original dataframe
        df = gdf(
            n=n, n_copies=6, seed=1, date_start="2021-01-01", date_end="2021-12-31"
        )
        # 1a: keep nans
        test_table = "test_tableB1a"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        # prepare to split dates; keep nans
        date_split = pd.to_datetime("2021-10-01", format="%Y-%m-%d")
        df1 = df[(df[date_col] <= date_split) | df[date_col].isna()]
        df2 = df[(df[date_col] >= date_split)]
        # upload and checks on df1
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df1,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col, symbol_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            df1 = df1.sort_values(
                by=[date_col, symbol_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df1))
            self.assertEqual(df_read.shape[1], len(df1.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df1, cat_col_to_exclude=symbol_col
            )
        # upload and checks on df2
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df2,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after second upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col, symbol_col, "float"], ascending=True, na_position="last"
            ).reset_index(drop=True)
            df = df.sort_values(
                by=[date_col, symbol_col, "float"], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df))
            self.assertEqual(df_read.shape[1], len(df1.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )

        # 1b: !keep_nans
        # create original dataframe
        df = gdf(
            n=n, n_copies=6, seed=1, date_start="2021-01-01", date_end="2021-12-31"
        )
        test_table = "test_tableB1b"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        # prepare to split dates; do not keep nans
        date_split = pd.to_datetime("2021-10-01", format="%Y-%m-%d")
        df = df[~(df[date_col].isna() | df[symbol_col].isna())]
        df1 = df[(df[date_col] <= date_split)]
        df2 = df[(df[date_col] >= date_split)]
        # upload and checks on df1
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df1,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col, symbol_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            df1 = df1.sort_values(
                by=[date_col, symbol_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # assert there are no nans in date_col and symbol_col
            self.assertEqual(df_read[date_col].isna().sum(), 0)
            self.assertEqual(df_read[symbol_col].isna().sum(), 0)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df1))
            self.assertEqual(df_read.shape[1], len(df1.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df1, cat_col_to_exclude=symbol_col
            )
        # upload and checks on df2
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df2,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after second upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col, symbol_col, "float"], ascending=True, na_position="last"
            ).reset_index(drop=True)
            df = df.sort_values(
                by=[date_col, symbol_col, "float"], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # assert there are no nans in date_col and symbol_col
            self.assertEqual(df_read[date_col].isna().sum(), 0)
            self.assertEqual(df_read[symbol_col].isna().sum(), 0)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df))
            self.assertEqual(df_read.shape[1], len(df2.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )

        # %% 2: keep nans, !set index: no nans should be dropped
        n = 502
        test_table = "test_tableB2"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        # prepare dataframes
        df1 = gdf(
            n=n, n_copies=6, seed=2, date_start="2021-01-01", date_end="2021-12-31"
        )
        df2 = gdf(
            n=n, n_copies=6, seed=2, date_start="2022-01-01", date_end="2023-12-31"
        )
        # upload and checks on df1
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df1,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df1))
            self.assertEqual(df_read.shape[1], len(df1.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df1, cat_col_to_exclude=symbol_col
            )
        # upload and checks on df2
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df2,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=False,
            set_index_symbol_col=False,
            update_latest=False,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=True,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after second upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # join df1 and df2 to form dataframe to compare with
            df = pd.concat([df1, df2], axis=0).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df))
            self.assertEqual(df_read.shape[1], len(df.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df, cat_col_to_exclude=symbol_col
            )

        # %% 3: update existing non-empty table, !keep_nans, update_latest; !use symbol_col as index
        n = 503
        # create original dataframe
        df = gdf(
            n=n, n_copies=1, seed=3, date_start="2021-01-01", date_end="2021-12-31"
        )
        test_table = "test_tableB3"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        # prepare to split dates; keep nans
        date_split = pd.to_datetime("2021-10-01", format="%Y-%m-%d")
        df1 = df[(df[date_col] < date_split)]
        df2 = df[(df[date_col] >= date_split)]
        # add observation to df1 that is supposed to be updated
        d_before_update = {
            "datetime": date_split,
            "int": 1000,
            "float": 1000.0,
            "str": "ZZZ",
        }
        df1.loc[len(df)] = d_before_update
        d_after_update = {
            "datetime": date_split,
            "int": -1000,
            "float": -1000.0,
            "str": "AAA",
        }
        df2.loc[len(df)] = d_after_update
        # upload and checks on df1
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df1,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=False,
            update_latest=True,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            df1 = df1.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df1))
            self.assertEqual(df_read.shape[1], len(df1.columns))
            # test that the latest value is as expected
            for key, exp_val in (
                df_read.loc[df_read[date_col].idxmax()].to_dict().items()
            ):
                self.assertEqual(
                    exp_val,
                    d_before_update[key],
                    msg="value before update is not as expected",
                )
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df1, cat_col_to_exclude=symbol_col
            )
        # upload and checks on df2
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df2,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=False,
            update_latest=True,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after second upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # add row after update to df and sort
            df_updated = df.copy()
            df_updated.loc[len(df)] = d_after_update
            df_updated = df_updated.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            df_updated = df_updated[~df_updated[date_col].isna()]
            # expected shape
            self.assertEqual(df_read.shape[0], len(df_updated))
            self.assertEqual(df_read.shape[1], len(df_updated.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df_updated, cat_col_to_exclude=symbol_col
            )

        # %% 4: update existing non-empty table, !keep_nans, update_latest; use symbol_col as index
        n = 504
        # create original dataframe
        df = gdf(
            n=n, n_copies=1, seed=4, date_start="2021-01-01", date_end="2021-12-31"
        )
        test_table = "test_tableB4"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        # prepare to split dates; keep nans
        date_split = pd.to_datetime("2021-10-01", format="%Y-%m-%d")
        df1 = df[(df[date_col] < date_split)]
        df2 = df[(df[date_col] >= date_split)]
        # add observation to df1 that is supposed to be updated
        d_before_update = {
            "datetime": date_split,
            "int": 1000,
            "float": 1000.0,
            "str": "ZZZ",
        }
        df1.loc[len(df)] = d_before_update
        d_after_update = {
            "datetime": date_split,
            "int": -1000,
            "float": -1000.0,
            "str": "AAA",
        }
        df2.loc[len(df)] = d_after_update

        # upload and checks on df1
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df1,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped; drop nan in symbols for df1
            df_read = df_read.sort_values(
                by=[date_col, symbol_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            df1 = df1[~df1[symbol_col].isna()]
            df1 = df1.sort_values(
                by=[date_col, symbol_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df1))
            self.assertEqual(df_read.shape[1], len(df1.columns))
            # test that the latest value is as expected
            for key, exp_val in (
                df_read.loc[df_read[date_col].idxmax()].to_dict().items()
            ):
                self.assertEqual(
                    exp_val,
                    d_before_update[key],
                    msg="value before update is not as expected",
                )
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df1, cat_col_to_exclude=symbol_col
            )
        # upload and checks on df2
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df2,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after second upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # add row after update to df and sort; drop nans in symbols
            df_updated = df[~df[date_col].isna() & ~df[symbol_col].isna()].copy()
            df_updated.loc[len(df)] = d_after_update
            df_updated = df_updated.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df_updated))
            self.assertEqual(df_read.shape[1], len(df_updated.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df_updated, cat_col_to_exclude=symbol_col
            )

        # %%# 5: update existing non-empty table, !keep_nans, update_latest; use symbol_col as index;
        #   give symbol; !raise_exception_overwrite_symbol_col - existing symbol column to be overwritten
        n = 505
        # create original dataframe
        df = gdf(
            n=n, n_copies=1, seed=5, date_start="2021-01-01", date_end="2021-12-31"
        )
        test_table = "test_tableB5"
        symbol = "AAA"
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        # prepare to split dates; keep nans
        date_split = pd.to_datetime("2021-10-01", format="%Y-%m-%d")
        df1 = df[(df[date_col] < date_split)]
        df2 = df[(df[date_col] >= date_split)]
        # add observation to df1 that is supposed to be updated
        d_before_update = {
            "datetime": date_split,
            "int": 1000,
            "float": 1000.0,
            "str": "AAA",
        }
        df1.loc[len(df)] = d_before_update
        d_after_update = {
            "datetime": date_split,
            "int": -1000,
            "float": -1000.0,
            "str": "AAA",
        }
        df2.loc[len(df)] = d_after_update

        # upload and checks on df1
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df1,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=False,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col, symbol_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # sort consistently as there are nans and the old index has been dropped; overwrite symbol column
            df1[symbol_col] = symbol
            df1 = df1.sort_values(
                by=[date_col, symbol_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df1))
            self.assertEqual(df_read.shape[1], len(df1.columns))
            # test that the latest value is as expected
            for key, exp_val in (
                df_read.loc[df_read[date_col].idxmax()].to_dict().items()
            ):
                self.assertEqual(
                    exp_val,
                    d_before_update[key],
                    msg="value before update is not as expected",
                )
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df1, cat_col_to_exclude=symbol_col
            )
        # upload and checks on df2
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df2,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=False,
            silent=False,
        )
        # checks on values after second upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # sort consistently as there are nans and the old index has been dropped;
            #   add row after update to df and sort; overwrite symbol column
            df_updated = df[~df[date_col].isna()].copy()
            df_updated[symbol_col] = symbol
            df_updated.loc[len(df)] = d_after_update
            df_updated = df_updated.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df_updated))
            self.assertEqual(df_read.shape[1], len(df_updated.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df_updated, cat_col_to_exclude=symbol_col
            )

        # %%# 6: update existing non-empty table, !keep_nans, update_latest; use symbol_col as index;
        #   give symbol; !raise_exception_overwrite_symbol_col - new symbol column
        n = 506
        # create original dataframe
        df = gdf(
            n=n, n_copies=1, seed=6, date_start="2021-01-01", date_end="2021-12-31"
        )
        test_table = "test_tableB6"
        symbol = "AAA"
        date_col = "datetime"
        symbol_col = "new_symbol_column"
        dtype_map = None
        # prepare to split dates; keep nans
        date_split = pd.to_datetime("2021-10-01", format="%Y-%m-%d")
        df1 = df[(df[date_col] < date_split)]
        df2 = df[(df[date_col] >= date_split)]
        # add observation to df1 that is supposed to be updated
        d_before_update = {
            "datetime": date_split,
            "int": 1000,
            "float": 1000.0,
            "str": "AAA",
            symbol_col: symbol,
        }
        df1.loc[len(df)] = d_before_update
        d_after_update = {
            "datetime": date_split,
            "int": -1000,
            "float": -1000.0,
            "str": "AAA",
            symbol_col: symbol,
        }
        df2.loc[len(df)] = d_after_update

        # upload and checks on df1
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df1,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped; drop nan in symbols for df1
            df_read = df_read.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # set new symbol column for df1, sort values
            df1[symbol_col] = symbol
            df1 = df1.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df1))
            self.assertEqual(df_read.shape[1], len(df1.columns))
            # test that the latest value is as expected
            for key, exp_val in (
                df_read.loc[df_read[date_col].idxmax()].to_dict().items()
            ):
                self.assertEqual(
                    exp_val,
                    d_before_update[key],
                    msg="value before update is not as expected",
                )
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df1, cat_col_to_exclude=symbol_col
            )
        # upload and checks on df2
        db.upload_df(
            engine=self.engine,
            symbol=symbol,
            df=df2,
            table_name=test_table,
            categorical_cols=None,
            numeric_cols=None,
            date_col=date_col,
            symbol_col=symbol_col,
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=False,
            columns_to_drop=None,
            dtype_map=dtype_map,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after second upload
        with self.engine.connect() as connection:
            # read from database
            df_read = pd.read_sql(test_table, connection)
            # sort consistently as there are nans and the old index has been dropped
            df_read = df_read.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # add row after update to df and sort; drop nans in symbols
            df_updated = df[~df[date_col].isna()].copy()
            df_updated[symbol_col] = symbol
            df_updated.loc[len(df)] = d_after_update
            df_updated = df_updated.sort_values(
                by=[date_col], ascending=True, na_position="last"
            ).reset_index(drop=True)
            # expected shape
            self.assertEqual(df_read.shape[0], len(df_updated))
            self.assertEqual(df_read.shape[1], len(df_updated.columns))
            # check equivalence of dataframes
            self.check_df_equivalence(
                df1=df_read, df2=df_updated, cat_col_to_exclude=symbol_col
            )

        # %% 7: Update empty table
        n = 507
        test_table = "test_tableB7"
        # prepare dataframe
        df = gdf(
            n=n, n_copies=2, seed=7, date_start="2020-01-01", date_end="2021-12-31"
        )
        # lists to iterate over
        list_update_latest = [True, False]
        list_columns_to_drop = [None, ["float"]]
        list_alter_table = [True, False]
        list_keep_keys_nans = [True, False]
        list_silent = [False]
        list_set_index_date_col = [True, False]
        list_set_index_symbol_col = [True, False]
        itr = 0
        for element in itertools.product(
            list_update_latest,
            list_alter_table,
            list_columns_to_drop,
            list_keep_keys_nans,
            list_silent,
            list_set_index_date_col,
            list_set_index_symbol_col,
        ):
            itr += 1
            with self.subTest(element=element):
                # Initialise iterations
                update_latest = element[0]
                alter_table = element[1]
                columns_to_drop = element[2]
                keep_keys_nans = element[3]
                silent = element[4]
                set_index_date_col = element[5]
                set_index_symbol_col = element[6]

                # a: with date_col and symbol_col
                # set other parameters
                symbol = None
                date_col = "datetime"
                symbol_col = "str"
                dtype_map = None
                # prepare dataframe to create table from: drop columns
                df_create_table = df.copy()
                if columns_to_drop is not None:
                    df_create_table = df_create_table.drop(columns=columns_to_drop)
                # prepare dataframe to compare with: drop columns, sort, and drop nans
                df_test = df.copy()
                if columns_to_drop is not None:
                    df_test = df_test.drop(columns=columns_to_drop)
                if set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[
                            ~df_test[date_col].isna() & ~df_test[symbol_col].isna()
                        ].copy()
                    df_test = df_test.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif set_index_date_col and not set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[~df_test[date_col].isna()].copy()
                    df_test = df_test.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif not set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[~df_test[symbol_col].isna()].copy()
                    df_test = df_test.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                else:
                    pass

                # create empty table
                self.assertIsNone(
                    db.create_table(
                        engine=self.engine,
                        table_name=f"{test_table}a-{itr}",
                        categorical_cols=None,
                        numeric_cols=None,
                        date_col=date_col,
                        symbol_col=symbol_col,
                        set_index_date_col=set_index_date_col,
                        set_index_symbol_col=set_index_symbol_col,
                        df=df_create_table,
                        dtype_map=dtype_map,
                    )
                )
                # upload
                db.upload_df(
                    engine=self.engine,
                    symbol=symbol,
                    df=df,
                    table_name=f"{test_table}a-{itr}",
                    categorical_cols=None,
                    numeric_cols=None,
                    date_col=date_col,
                    symbol_col=symbol_col,
                    drop_index_cols=True,
                    set_index_date_col=set_index_date_col,
                    set_index_symbol_col=set_index_symbol_col,
                    update_latest=update_latest,
                    alter_table=alter_table,
                    columns_to_drop=columns_to_drop,
                    dtype_map=dtype_map,
                    keep_keys_nans=keep_keys_nans,
                    raise_exception_keys_nans=False,
                    raise_exception_overwrite_symbol_col=True,
                    silent=silent,
                )
                # checks on values after first upload
                with self.engine.connect() as connection:
                    # read from database and sort values
                    df_read = pd.read_sql(f"{test_table}a-{itr}", connection)
                    if not set_index_date_col and not set_index_symbol_col:
                        pass
                    else:
                        df_read = df_read.sort_values(
                            by=[date_col, symbol_col, "int"],
                            ascending=True,
                            na_position="last",
                        ).reset_index(drop=True)
                    # expected shape
                    self.assertEqual(df_read.shape[0], len(df_test))
                    self.assertEqual(df_read.shape[1], len(df_test.columns))
                    # check nans in date_col and symbol_col
                    if keep_keys_nans:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and not set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif not set_index_date_col and set_index_symbol_col:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    else:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    # check equivalence of dataframes
                    self.check_df_equivalence(
                        df1=df_read,
                        df2=df_test,
                        cat_col_to_exclude=symbol_col,
                    )

                # b: with date_col only
                # set other parameters
                symbol = None
                date_col = "datetime"
                symbol_col = None
                dtype_map = None
                # prepare dataframe to create table from: drop columns
                df_create_table = df.copy()
                if columns_to_drop is not None:
                    df_create_table = df_create_table.drop(columns=columns_to_drop)
                # prepare dataframe to compare with: drop columns, sort, and drop nans
                df_test = df.copy()
                if columns_to_drop is not None:
                    df_test = df_test.drop(columns=columns_to_drop)
                if set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[~df_test[date_col].isna()].copy()
                    df_test = df_test.sort_values(
                        by=[date_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif set_index_date_col and not set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[~df_test[date_col].isna()].copy()
                    df_test = df_test.sort_values(
                        by=[date_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif not set_index_date_col and set_index_symbol_col:
                    df_test = df_test.sort_values(
                        by=[date_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                else:
                    pass

                # create empty table
                self.assertIsNone(
                    db.create_table(
                        engine=self.engine,
                        table_name=f"{test_table}b-{itr}",
                        categorical_cols=None,
                        numeric_cols=None,
                        date_col=date_col,
                        symbol_col=symbol_col,
                        set_index_date_col=set_index_date_col,
                        set_index_symbol_col=set_index_symbol_col,
                        df=df_create_table,
                        dtype_map=dtype_map,
                    )
                )
                # upload
                db.upload_df(
                    engine=self.engine,
                    symbol=symbol,
                    df=df,
                    table_name=f"{test_table}b-{itr}",
                    categorical_cols=None,
                    numeric_cols=None,
                    date_col=date_col,
                    symbol_col=symbol_col,
                    drop_index_cols=True,
                    set_index_date_col=set_index_date_col,
                    set_index_symbol_col=set_index_symbol_col,
                    update_latest=update_latest,
                    alter_table=alter_table,
                    columns_to_drop=columns_to_drop,
                    dtype_map=dtype_map,
                    keep_keys_nans=keep_keys_nans,
                    raise_exception_keys_nans=False,
                    raise_exception_overwrite_symbol_col=True,
                    silent=silent,
                )
                # checks on values after first upload
                with self.engine.connect() as connection:
                    # read from database and sort values
                    df_read = pd.read_sql(f"{test_table}b-{itr}", connection)
                    if not set_index_date_col and not set_index_symbol_col:
                        pass
                    else:
                        df_read = df_read.sort_values(
                            by=[date_col, "int"],
                            ascending=True,
                            na_position="last",
                        ).reset_index(drop=True)
                    # expected shape
                    self.assertEqual(df_read.shape[0], len(df_test))
                    self.assertEqual(df_read.shape[1], len(df_test.columns))
                    # check nans in date_col and symbol_col
                    if keep_keys_nans:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                    elif set_index_date_col and set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                    elif set_index_date_col and not set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                    elif not set_index_date_col and set_index_symbol_col:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                    else:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                    # check equivalence of dataframes
                    self.check_df_equivalence(
                        df1=df_read,
                        df2=df_test,
                        cat_col_to_exclude=None,
                    )

                # c: with symbol_col only
                # set other parameters
                symbol = None
                date_col = None
                symbol_col = "str"
                dtype_map = None
                # prepare dataframe to create table from: drop columns
                df_create_table = df.copy()
                if columns_to_drop is not None:
                    df_create_table = df_create_table.drop(columns=columns_to_drop)
                # prepare dataframe to compare with: drop columns, sort, and drop nans
                df_test = df.copy()
                if columns_to_drop is not None:
                    df_test = df_test.drop(columns=columns_to_drop)
                if set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[~df_test[symbol_col].isna()].copy()
                    df_test = df_test.sort_values(
                        by=[symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif set_index_date_col and not set_index_symbol_col:
                    df_test = df_test.sort_values(
                        by=[symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif not set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[~df_test[symbol_col].isna()].copy()
                    df_test = df_test.sort_values(
                        by=[symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                else:
                    pass

                # create empty table
                self.assertIsNone(
                    db.create_table(
                        engine=self.engine,
                        table_name=f"{test_table}c-{itr}",
                        categorical_cols=None,
                        numeric_cols=None,
                        date_col=date_col,
                        symbol_col=symbol_col,
                        set_index_date_col=set_index_date_col,
                        set_index_symbol_col=set_index_symbol_col,
                        df=df_create_table,
                        dtype_map=dtype_map,
                    )
                )
                # upload
                db.upload_df(
                    engine=self.engine,
                    symbol=symbol,
                    df=df,
                    table_name=f"{test_table}c-{itr}",
                    categorical_cols=None,
                    numeric_cols=None,
                    date_col=date_col,
                    symbol_col=symbol_col,
                    drop_index_cols=True,
                    set_index_date_col=set_index_date_col,
                    set_index_symbol_col=set_index_symbol_col,
                    update_latest=update_latest,
                    alter_table=alter_table,
                    columns_to_drop=columns_to_drop,
                    dtype_map=dtype_map,
                    keep_keys_nans=keep_keys_nans,
                    raise_exception_keys_nans=False,
                    raise_exception_overwrite_symbol_col=True,
                    silent=silent,
                )
                # checks on values after first upload
                with self.engine.connect() as connection:
                    # read from database and sort values
                    df_read = pd.read_sql(f"{test_table}c-{itr}", connection)
                    if not set_index_date_col and not set_index_symbol_col:
                        pass
                    else:
                        df_read = df_read.sort_values(
                            by=[symbol_col, "int"],
                            ascending=True,
                            na_position="last",
                        ).reset_index(drop=True)
                    # expected shape
                    self.assertEqual(df_read.shape[0], len(df_test))
                    self.assertEqual(df_read.shape[1], len(df_test.columns))
                    # check nans in date_col and symbol_col
                    if keep_keys_nans:
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and set_index_symbol_col:
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and not set_index_symbol_col:
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif not set_index_date_col and set_index_symbol_col:
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    else:
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    # check equivalence of dataframes
                    self.check_df_equivalence(
                        df1=df_read,
                        df2=df_test,
                        cat_col_to_exclude=None,
                    )

                # d: no index columns
                # set other parameters
                symbol = None
                date_col = None
                symbol_col = None
                dtype_map = None
                # prepare dataframe to create table from: drop columns
                df_create_table = df.copy()
                if columns_to_drop is not None:
                    df_create_table = df_create_table.drop(columns=columns_to_drop)
                # prepare dataframe to compare with: drop columns, sort, and drop nans
                df_test = df.copy()
                if columns_to_drop is not None:
                    df_test = df_test.drop(columns=columns_to_drop)

                # create empty table
                self.assertIsNone(
                    db.create_table(
                        engine=self.engine,
                        table_name=f"{test_table}d-{itr}",
                        categorical_cols=None,
                        numeric_cols=None,
                        date_col=date_col,
                        symbol_col=symbol_col,
                        set_index_date_col=set_index_date_col,
                        set_index_symbol_col=set_index_symbol_col,
                        df=df_create_table,
                        dtype_map=dtype_map,
                    )
                )
                # upload
                db.upload_df(
                    engine=self.engine,
                    symbol=symbol,
                    df=df,
                    table_name=f"{test_table}d-{itr}",
                    categorical_cols=None,
                    numeric_cols=None,
                    date_col=date_col,
                    symbol_col=symbol_col,
                    drop_index_cols=True,
                    set_index_date_col=set_index_date_col,
                    set_index_symbol_col=set_index_symbol_col,
                    update_latest=update_latest,
                    alter_table=alter_table,
                    columns_to_drop=columns_to_drop,
                    dtype_map=dtype_map,
                    keep_keys_nans=keep_keys_nans,
                    raise_exception_keys_nans=False,
                    raise_exception_overwrite_symbol_col=True,
                    silent=silent,
                )
                # checks on values after first upload
                with self.engine.connect() as connection:
                    # read from database and sort values
                    df_read = pd.read_sql(f"{test_table}d-{itr}", connection)
                    # expected shape
                    self.assertEqual(df_read.shape[0], len(df_test))
                    self.assertEqual(df_read.shape[1], len(df_test.columns))
                    # check equivalence of dataframes
                    self.check_df_equivalence(
                        df1=df_read,
                        df2=df_test,
                        cat_col_to_exclude=None,
                    )

        # %% 8: test when the same dataframe is uploaded twice
        n = 108
        # create original dataframe
        df = gdf(
            n=n, n_copies=6, seed=8, date_start="2020-01-01", date_end="2021-12-31"
        )
        test_table = "test_tableB8"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        # lists to iterate over
        list_update_latest = [True, False]
        list_columns_to_drop = [None, ["float"]]
        list_alter_table = [True, False]
        list_keep_keys_nans = [True, False]
        list_silent = [False]
        list_set_index_date_col = [True, False]
        list_set_index_symbol_col = [True, False]
        itr = 0
        for element in itertools.product(
            list_update_latest,
            list_alter_table,
            list_columns_to_drop,
            list_keep_keys_nans,
            list_silent,
            list_set_index_date_col,
            list_set_index_symbol_col,
        ):
            itr += 1
            with self.subTest(element=element):
                # Initialise iterations
                update_latest = element[0]
                alter_table = element[1]
                columns_to_drop = element[2]
                keep_keys_nans = element[3]
                silent = element[4]
                set_index_date_col = element[5]
                set_index_symbol_col = element[6]

                # prepare dataframe to compare with: drop columns, sort, and drop nans
                df_test = df.copy()
                if columns_to_drop is not None:
                    df_test = df_test.drop(columns=columns_to_drop)
                if set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[
                            ~df_test[date_col].isna() & ~df_test[symbol_col].isna()
                        ].copy()
                    df_test = df_test.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif set_index_date_col and not set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[~df_test[date_col].isna()].copy()
                    df_test = df_test.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif not set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test = df_test[~df_test[symbol_col].isna()].copy()
                    df_test = df_test.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                else:
                    df_test = df_test.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)

                # upload and checks on df
                db.upload_df(
                    engine=self.engine,
                    symbol=symbol,
                    df=df,
                    table_name=f"{test_table}-{itr}",
                    categorical_cols=None,
                    numeric_cols=None,
                    date_col=date_col,
                    symbol_col=symbol_col,
                    drop_index_cols=True,
                    set_index_date_col=set_index_date_col,
                    set_index_symbol_col=set_index_symbol_col,
                    update_latest=update_latest,
                    alter_table=alter_table,
                    columns_to_drop=columns_to_drop,
                    dtype_map=dtype_map,
                    keep_keys_nans=keep_keys_nans,
                    raise_exception_keys_nans=False,
                    raise_exception_overwrite_symbol_col=True,
                    silent=silent,
                )
                # checks on values after first upload
                with self.engine.connect() as connection:
                    # read from database
                    df_read = pd.read_sql(f"{test_table}-{itr}", connection)
                    # sort consistently as there are nans and the old index has been dropped
                    df_read = df_read.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                    # check nans in date_col and symbol_col
                    if keep_keys_nans:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and not set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif not set_index_date_col and set_index_symbol_col:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    else:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    # expected shape
                    self.assertEqual(df_read.shape[0], len(df_test))
                    self.assertEqual(df_read.shape[1], len(df_test.columns))
                    # check equivalence of dataframes
                    self.check_df_equivalence(
                        df1=df_read, df2=df_test, cat_col_to_exclude=symbol_col
                    )
                # upload again and perform checks on df
                db.upload_df(
                    engine=self.engine,
                    symbol=symbol,
                    df=df,
                    table_name=f"{test_table}-{itr}",
                    categorical_cols=None,
                    numeric_cols=None,
                    date_col=date_col,
                    symbol_col=symbol_col,
                    drop_index_cols=True,
                    set_index_date_col=True,
                    set_index_symbol_col=True,
                    update_latest=update_latest,
                    alter_table=alter_table,
                    columns_to_drop=columns_to_drop,
                    dtype_map=dtype_map,
                    keep_keys_nans=False,
                    raise_exception_keys_nans=False,
                    raise_exception_overwrite_symbol_col=True,
                    silent=silent,
                )
                # checks on values after first upload
                with self.engine.connect() as connection:
                    # read from database
                    df_read = pd.read_sql(f"{test_table}-{itr}", connection)
                    # sort consistently as there are nans and the old index has been dropped
                    df_read = df_read.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                    # check nans in date_col and symbol_col
                    if keep_keys_nans:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and not set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif not set_index_date_col and set_index_symbol_col:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    else:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    # expected shape
                    self.assertEqual(df_read.shape[0], len(df_test))
                    self.assertEqual(df_read.shape[1], len(df_test.columns))
                    # check equivalence of dataframes
                    self.check_df_equivalence(
                        df1=df_read, df2=df_test, cat_col_to_exclude=symbol_col
                    )

        # %% 9: alter table while updating
        n = 109
        # create original dataframes (no overlap in time)
        df1 = gdf(
            n=n, n_copies=6, seed=8, date_start="2020-01-01", date_end="2021-12-31"
        )
        df2 = gdf(
            n=n, n_copies=8, seed=8, date_start="2022-01-01", date_end="2022-12-31"
        )
        test_table = "test_tableB9"
        symbol = None
        date_col = "datetime"
        symbol_col = "str"
        dtype_map = None
        # lists to iterate over
        list_update_latest = [True, False]
        list_columns_to_drop = [None, ["float"]]
        list_keep_keys_nans = [True, False]
        list_silent = [False]
        list_set_index_date_col = [True, False]
        list_set_index_symbol_col = [True, False]
        itr = 0
        for element in itertools.product(
            list_update_latest,
            list_columns_to_drop,
            list_keep_keys_nans,
            list_silent,
            list_set_index_date_col,
            list_set_index_symbol_col,
        ):
            itr += 1
            with self.subTest(element=element):
                # Initialise iterations
                update_latest = element[0]
                columns_to_drop = element[1]
                keep_keys_nans = element[2]
                silent = element[3]
                set_index_date_col = element[4]
                set_index_symbol_col = element[5]

                # upload and checks on df
                db.upload_df(
                    engine=self.engine,
                    symbol=symbol,
                    df=df1,
                    table_name=f"{test_table}-{itr}",
                    categorical_cols=None,
                    numeric_cols=None,
                    date_col=date_col,
                    symbol_col=symbol_col,
                    drop_index_cols=True,
                    set_index_date_col=set_index_date_col,
                    set_index_symbol_col=set_index_symbol_col,
                    update_latest=update_latest,
                    alter_table=True,
                    columns_to_drop=columns_to_drop,
                    dtype_map=dtype_map,
                    keep_keys_nans=keep_keys_nans,
                    raise_exception_keys_nans=False,
                    raise_exception_overwrite_symbol_col=True,
                    silent=silent,
                )
                # prepare dataframe to compare with: drop columns, sort, and drop nans
                df_test1 = df1.copy()
                if columns_to_drop is not None:
                    df_test1 = df_test1.drop(columns=columns_to_drop)
                if set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test1 = df_test1[
                            ~df_test1[date_col].isna() & ~df_test1[symbol_col].isna()
                        ].copy()
                    df_test1 = df_test1.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif set_index_date_col and not set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test1 = df_test1[~df_test1[date_col].isna()].copy()
                    df_test1 = df_test1.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                elif not set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test1 = df_test1[~df_test1[symbol_col].isna()].copy()
                    df_test1 = df_test1.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                else:
                    df_test1 = df_test1.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                # checks on values after first upload
                with self.engine.connect() as connection:
                    # read from database
                    df_read = pd.read_sql(f"{test_table}-{itr}", connection)
                    # sort consistently as there are nans and the old index has been dropped
                    df_read = df_read.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                    # check nans in date_col and symbol_col
                    if keep_keys_nans:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and not set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif not set_index_date_col and set_index_symbol_col:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    else:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    # check expected shape
                    self.assertEqual(df_read.shape[0], len(df_test1))
                    self.assertEqual(df_read.shape[1], len(df_test1.columns))
                    # check equivalence of dataframes
                    self.check_df_equivalence(
                        df1=df_read, df2=df_test1, cat_col_to_exclude=symbol_col
                    )

                # upload the second dataframe with more columns and check again
                db.upload_df(
                    engine=self.engine,
                    symbol=symbol,
                    df=df2,
                    table_name=f"{test_table}-{itr}",
                    categorical_cols=None,
                    numeric_cols=None,
                    date_col=date_col,
                    symbol_col=symbol_col,
                    drop_index_cols=True,
                    set_index_date_col=set_index_date_col,
                    set_index_symbol_col=set_index_symbol_col,
                    update_latest=update_latest,
                    alter_table=True,
                    columns_to_drop=columns_to_drop,
                    dtype_map=dtype_map,
                    keep_keys_nans=keep_keys_nans,
                    raise_exception_keys_nans=False,
                    raise_exception_overwrite_symbol_col=True,
                    silent=silent,
                )
                # prepare dataframe to compare with: drop columns, sort, and drop nans
                df_test2 = df2.copy()
                if columns_to_drop is not None:
                    df_test2 = df_test2.drop(columns=columns_to_drop)
                if set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test2 = df_test2[
                            ~df_test2[date_col].isna() & ~df_test2[symbol_col].isna()
                        ].copy()
                elif set_index_date_col and not set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test2 = df_test2[~df_test2[date_col].isna()].copy()
                elif not set_index_date_col and set_index_symbol_col:
                    if keep_keys_nans:
                        pass
                    else:
                        df_test2 = df_test2[~df_test2[symbol_col].isna()].copy()
                else:
                    pass
                # join test-data
                df_test2 = pd.concat([df_test1, df_test2], ignore_index=True)
                df_test2 = df_test2.sort_values(
                    by=[date_col, symbol_col, "int"],
                    ascending=True,
                    na_position="last",
                ).reset_index(drop=True)
                # checks on values after second upload
                with self.engine.connect() as connection:
                    # read from database
                    df_read = pd.read_sql(f"{test_table}-{itr}", connection)
                    # sort consistently as there are nans and the old index has been dropped
                    df_read = df_read.sort_values(
                        by=[date_col, symbol_col, "int"],
                        ascending=True,
                        na_position="last",
                    ).reset_index(drop=True)
                    # check nans in date_col and symbol_col
                    if keep_keys_nans:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    elif set_index_date_col and not set_index_symbol_col:
                        self.assertEqual(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    elif not set_index_date_col and set_index_symbol_col:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertEqual(df_read[symbol_col].isna().sum(), 0)
                    else:
                        self.assertGreater(df_read[date_col].isna().sum(), 0)
                        self.assertGreater(df_read[symbol_col].isna().sum(), 0)
                    # check expected shape
                    self.assertEqual(df_read.shape[0], len(df_test2))
                    self.assertEqual(df_read.shape[1], len(df_test2.columns))
                    # check equivalence of dataframes
                    self.check_df_equivalence(
                        df1=df_read, df2=df_test2, cat_col_to_exclude=symbol_col
                    )

    # %% Part C: special cases and expected exceptions
    @unittest.skipIf(skip_upload_df, "skipping all tests involving upload_df")
    def test_upload_df_special_cases(self):
        """
        Special cases
        """
        # 1: Empty table/no db_latest_date, upload df that does not filter to empty
        n = 200
        df = gdf(n=n, n_copies=5, seed=1)
        # create empty table
        self.assertIsNone(
            db.create_table(
                engine=self.engine,
                table_name="test_table_zzz_sc0",
                categorical_cols=None,
                numeric_cols=None,
                date_col="datetime",
                symbol_col="str",
                set_index_date_col=True,
                set_index_symbol_col=True,
                df=df,
                dtype_map=None,
            )
        )
        # upload
        db.upload_df(
            engine=self.engine,
            symbol=None,
            df=df,
            table_name="test_table_zzz_sc0",
            categorical_cols=None,
            numeric_cols=None,
            date_col="datetime",
            symbol_col="str",
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=True,
            columns_to_drop=None,
            dtype_map=None,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after uploading
        with self.engine.connect() as connection:
            # read from database and sort values
            df_read = pd.read_sql("test_table_zzz_sc0", connection)
            df_read = df_read.sort_values(
                by=["datetime", "str", "int"],
                ascending=True,
                na_position="last",
            ).reset_index(drop=True)
            # expected shape
            self.assertGreater(df_read.shape[0], 0)
            self.assertGreater(df_read.shape[1], 0)

        # 2: Empty table/no db_latest_date, upload df that filters to empty
        # create a special data frame where there is no row where neither the date nor symbol cols are non-nan
        df = pd.DataFrame(
            {
                "datetime": [datetime.datetime(2020, 2, 2), np.nan, np.nan],
                "symbol": [np.nan, "b", "c"],
            }
        )
        # create empty table
        self.assertIsNone(
            db.create_table(
                engine=self.engine,
                table_name="test_table_zzz_sc1",
                categorical_cols=None,
                numeric_cols=None,
                date_col="datetime",
                symbol_col="symbol",
                set_index_date_col=True,
                set_index_symbol_col=True,
                df=df,
                dtype_map=None,
            )
        )
        # upload
        db.upload_df(
            engine=self.engine,
            symbol=None,
            df=df,
            table_name="test_table_zzz_sc1",
            categorical_cols=None,
            numeric_cols=None,
            date_col="datetime",
            symbol_col="symbol",
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=True,
            columns_to_drop=None,
            dtype_map=None,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after uploading
        with self.engine.connect() as connection:
            # read from database and sort values
            df_read = pd.read_sql("test_table_zzz_sc1", connection)
            # expected shape
            self.assertEqual(df_read.shape[0], 0)
            self.assertEqual(df_read.shape[1], 2)

        # 3: Upload non-empty data frame (update requested), which is then uploaded again, but with older data does not change the table
        df0 = pd.DataFrame(
            {
                "datetime": [
                    datetime.datetime(2020, 2, 2),
                    datetime.datetime(2020, 2, 3),
                    datetime.datetime(2020, 2, 4),
                ],
                "symbol": [np.nan, "b", "c"],
            }
        )
        df1 = pd.DataFrame(
            {
                "datetime": [
                    datetime.datetime(2020, 2, 1),
                    datetime.datetime(2020, 2, 2),
                    datetime.datetime(2020, 2, 3),
                ],
                "symbol": ["d", "e", "f"],
            }
        )
        # first upload
        db.upload_df(
            engine=self.engine,
            symbol=None,
            df=df0,
            table_name="test_table_zzz_sc2",
            categorical_cols=None,
            numeric_cols=None,
            date_col="datetime",
            symbol_col="symbol",
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=True,
            columns_to_drop=None,
            dtype_map=None,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database and sort values
            df_read0 = pd.read_sql("test_table_zzz_sc2", connection)
            # expected shape
            self.assertEqual(df_read0.shape[0], 2)
            self.assertEqual(df_read0.shape[1], 2)
        # second upload
        db.upload_df(
            engine=self.engine,
            symbol=None,
            df=df1,
            table_name="test_table_zzz_sc2",
            categorical_cols=None,
            numeric_cols=None,
            date_col="datetime",
            symbol_col="symbol",
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=True,
            alter_table=True,
            columns_to_drop=None,
            dtype_map=None,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database and sort values
            df_read1 = pd.read_sql("test_table_zzz_sc2", connection)
            # expected shape
            self.assertEqual(df_read1.shape[0], 2)
            self.assertEqual(df_read1.shape[1], 2)
            # check that the table did not change
            self.assertIsNone(pd.testing.assert_frame_equal(df_read0, df_read1))

        # 4: Upload non-empty data frame (no update requested), which is then uploaded again, but with older data does not change the table
        df0 = pd.DataFrame(
            {
                "datetime": [
                    datetime.datetime(2021, 2, 2),
                    datetime.datetime(2021, 2, 3),
                    datetime.datetime(2021, 2, 4),
                ],
                "symbol": [np.nan, "b", "c"],
            }
        )
        df1 = pd.DataFrame(
            {
                "datetime": [
                    datetime.datetime(2021, 2, 1),
                    datetime.datetime(2021, 2, 2),
                    datetime.datetime(2021, 2, 3),
                ],
                "symbol": ["d", "e", "f"],
            }
        )
        # first upload
        db.upload_df(
            engine=self.engine,
            symbol=None,
            df=df0,
            table_name="test_table_zzz_sc3",
            categorical_cols=None,
            numeric_cols=None,
            date_col="datetime",
            symbol_col="symbol",
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=True,
            columns_to_drop=None,
            dtype_map=None,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database and sort values
            df_read0 = pd.read_sql("test_table_zzz_sc3", connection)
            # expected shape
            self.assertEqual(df_read0.shape[0], 2)
            self.assertEqual(df_read0.shape[1], 2)
        # second upload
        db.upload_df(
            engine=self.engine,
            symbol=None,
            df=df1,
            table_name="test_table_zzz_sc3",
            categorical_cols=None,
            numeric_cols=None,
            date_col="datetime",
            symbol_col="symbol",
            drop_index_cols=True,
            set_index_date_col=True,
            set_index_symbol_col=True,
            update_latest=False,
            alter_table=True,
            columns_to_drop=None,
            dtype_map=None,
            keep_keys_nans=False,
            raise_exception_keys_nans=False,
            raise_exception_overwrite_symbol_col=True,
            silent=False,
        )
        # checks on values after first upload
        with self.engine.connect() as connection:
            # read from database and sort values
            df_read1 = pd.read_sql("test_table_zzz_sc3", connection)
            # expected shape
            self.assertEqual(df_read1.shape[0], 2)
            self.assertEqual(df_read1.shape[1], 2)
            # check that the table did not change
            self.assertIsNone(pd.testing.assert_frame_equal(df_read0, df_read1))

    @unittest.skipIf(skip_upload_df, "skipping all tests involving upload_df")
    def test_upload_df_exceptions(self):
        """
        Tests involving expected exceptions
        """
        # 1: Exceptions from inputs
        n = 501
        df = gdf(n=n, n_copies=5, seed=1)
        """
        self.assertRaises(
            AssertionError,
            db.upload_df,
            **{
                "engine": self.engine,
                "symbol": symbol,
                "df": df,
                "table_name": "test_table_zzz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": None,
                "symbol_col": "str_1",
                "drop_index_cols": True,
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": True,
                "raise_exception_keys_nans": True,
                "raise_exception_overwrite_symbol_col": False,
                "silent": False
            },
        )
        """

        self.assertRaises(
            AssertionError,
            db.upload_df,
            **{
                "engine": "STRING DOES NOT WORK",
                "symbol": "symbol",
                "df": df,
                "table_name": "test_table_zzz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": None,
                "symbol_col": "str_1",
                "drop_index_cols": True,
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": True,
                "raise_exception_keys_nans": True,
                "raise_exception_overwrite_symbol_col": False,
                "silent": False,
            },
        )
        self.assertRaises(
            AssertionError,
            db.upload_df,
            **{
                "engine": self.engine,
                "symbol": ["LIST DOES NOT WORK"],
                "df": df,
                "table_name": "test_table_zzz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": None,
                "symbol_col": "str_1",
                "drop_index_cols": True,
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": True,
                "raise_exception_keys_nans": True,
                "raise_exception_overwrite_symbol_col": False,
                "silent": False,
            },
        )
        self.assertRaises(
            ValueError,
            db.upload_df,
            **{
                "engine": self.engine,
                "symbol": "symbol",
                "df": pd.Series(),
                "table_name": "test_table_zzz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": None,
                "symbol_col": "str_1",
                "drop_index_cols": True,
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": True,
                "raise_exception_keys_nans": True,
                "raise_exception_overwrite_symbol_col": False,
                "silent": False,
            },
        )
        self.assertRaises(
            AssertionError,
            db.upload_df,
            **{
                "engine": self.engine,
                "symbol": "symbol",
                "df": df,
                "table_name": "",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": None,
                "symbol_col": "str_1",
                "drop_index_cols": True,
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": True,
                "raise_exception_keys_nans": True,
                "raise_exception_overwrite_symbol_col": False,
                "silent": False,
            },
        )
        # check 0.6
        self.assertRaises(
            ValueError,
            db.upload_df,
            **{
                "engine": self.engine,
                "symbol": "symbol",
                "df": df,
                "table_name": "test_table_zzz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": "datetime",
                "symbol_col": "str",
                "drop_index_cols": True,
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": False,
                "raise_exception_keys_nans": True,
                "raise_exception_overwrite_symbol_col": True,
                "silent": False,
            },
        )
        # check 0.7
        self.assertRaises(
            ValueError,
            db.upload_df,
            **{
                "engine": self.engine,
                "symbol": "symbol",
                "df": df,
                "table_name": "test_table_zzz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": None,
                "symbol_col": "str",
                "drop_index_cols": True,
                "set_index_date_col": True,
                "set_index_symbol_col": False,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": True,
                "raise_exception_keys_nans": True,
                "raise_exception_overwrite_symbol_col": True,
                "silent": False,
            },
        )
        self.assertRaises(
            ValueError,
            db.upload_df,
            **{
                "engine": self.engine,
                "symbol": None,
                "df": df,
                "table_name": "test_table_zzz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": "datetime",
                "symbol_col": "str",
                "drop_index_cols": False,
                "set_index_date_col": False,
                "set_index_symbol_col": True,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": False,
                "raise_exception_keys_nans": True,
                "raise_exception_overwrite_symbol_col": True,
                "silent": False,
            },
        )

        # 1: empty df and None
        self.assertIsNone(
            db.upload_df(
                engine=self.engine,
                symbol=None,
                df=None,
                table_name="test_table_zzz",
                categorical_cols=None,
                numeric_cols=None,
                date_col=None,
                symbol_col=None,
                drop_index_cols=True,
                set_index_date_col=False,
                set_index_symbol_col=False,
                update_latest=False,
                alter_table=False,
                columns_to_drop=None,
                dtype_map=None,
                keep_keys_nans=True,
                raise_exception_keys_nans=False,
                raise_exception_overwrite_symbol_col=False,
                silent=False,
            )
        )
        self.assertIsNone(
            db.upload_df(
                engine=self.engine,
                symbol=None,
                df=pd.DataFrame(),
                table_name="test_table_zzz",
                categorical_cols=None,
                numeric_cols=None,
                date_col=None,
                symbol_col=None,
                drop_index_cols=True,
                set_index_date_col=False,
                set_index_symbol_col=False,
                update_latest=False,
                alter_table=False,
                columns_to_drop=None,
                dtype_map=None,
                keep_keys_nans=True,
                raise_exception_keys_nans=False,
                raise_exception_overwrite_symbol_col=False,
                silent=False,
            )
        )
        self.assertRaises(
            ValueError,
            db.upload_df,
            **{
                "engine": self.engine,
                "symbol": "symbol",
                "df": df,
                "table_name": "test_table_zzz",
                "categorical_cols": None,
                "numeric_cols": None,
                "date_col": None,
                "symbol_col": "str",
                "drop_index_cols": True,
                "set_index_date_col": True,
                "set_index_symbol_col": True,
                "update_latest": True,
                "alter_table": True,
                "columns_to_drop": None,
                "dtype_map": None,
                "keep_keys_nans": True,
                "raise_exception_keys_nans": False,
                "raise_exception_overwrite_symbol_col": True,
                "silent": False,
            },
        )


if __name__ == "__main__":
    unittest.main()
