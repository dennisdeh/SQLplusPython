import datetime
import time
import unittest
import sklearn.preprocessing as pp
import sqlpluspython.db_connection as db
import numpy as np
import itertools
from sqlpluspython.testing.aa_cleaner.clean_test_database import (
    reset_test_tables,
)
from sqlpluspython.utils.generate_test_data import gdf


class DatabaseFunctionsParameters(unittest.TestCase):
    """
    Tests of functions for interacting with the database MariaDB
    SQL backend for reading/writing tables involving data
    transformations
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

        # always reset the testing database before running tests
        reset_test_tables(engine=cls.engine, confirm=True)

    @classmethod
    def tearDownClass(cls):
        """
        Reset testing database and disposal of the SQL engine
        """
        # if cls.reset_db:
        #     reset_test_tables(engine=cls.engine, confirm=True)
        cls.engine.dispose(close=True)

    def check_objects(self, obj1, obj2):
        self.assertEqual(len(obj1.__getstate__()), len(obj2.__getstate__()))
        for key in obj1.__getstate__():
            if (
                isinstance(obj1.__getstate__()[key], bool)
                or isinstance(obj1.__getstate__()[key], str)
                or isinstance(obj1.__getstate__()[key], int)
            ):
                self.assertEqual(
                    obj1.__getstate__()[key],
                    obj2.__getstate__()[key],
                    msg=f"Not equal for: {key}",
                )
            elif isinstance(obj1.__getstate__()[key], list):
                self.assertListEqual(
                    obj1.__getstate__()[key],
                    obj2.__getstate__()[key],
                    msg=f"Not equal for: {key}",
                )
            elif isinstance(obj1.__getstate__()[key], np.ndarray):
                self.assertIsNone(
                    np.testing.assert_array_equal(
                        obj1.__getstate__()[key],
                        obj2.__getstate__()[key],
                    ),
                    msg=f"Not equal for: {key}",
                )
            else:
                print(f"{key}: not checked!")

    def test_get_object_transformer_date_symbol(self):
        """
        Test of upload and download of transformer objects
        """
        # initialise
        n = 0
        list_tables = ["trafo_test0", "trafo_test1"]
        list_seed = [0, 1, 2]
        list_method_preprocessing = [pp.StandardScaler(), pp.MinMaxScaler()]
        list_date_col = ["date", None]

        for element in itertools.product(
            list_tables, list_seed, list_method_preprocessing, list_date_col
        ):
            with self.subTest(element=element):
                # Initialise
                n += 1
                table_name = element[0]
                seed = element[1]
                tr = element[2]
                date_col = element[3]

                # 0: get test data (numerical columns only)
                df = gdf(
                    n=1000,
                    n_copies=5,
                    seed=seed,
                    ratio_nans=0.1,
                )
                df_num = df.select_dtypes(include=["number"])

                # train and transform
                tr = tr.fit(df_num)
                df_transformed = df.copy()
                df_transformed[df_num.columns] = tr.transform(df_num)

                # upload transformer
                db.upload_object(
                    engine=self.engine,
                    obj=tr,
                    table_name=f"{table_name}-{date_col is None}",
                    date=datetime.datetime.now(),
                    symbol=f"TST{n}",
                    date_col=date_col,
                    symbol_col="symbol",
                )

                # load transformation parameters again
                tr_read = db.get_object(
                    engine=self.engine,
                    table_name=f"{table_name}-{date_col is None}",
                    symbol=f"TST{n}",
                    date_col=date_col,
                    symbol_col="symbol",
                )

                # test unpickled sklearn object
                self.check_objects(tr, tr_read)
        # sleep
        time.sleep(0.1)

    def test_get_object_transformer_date_symbol_overwrite(self):
        """
        Test of upload and download of a transformer object,
        where the existing one will be overwritten
        """
        # 0: get test data (numerical columns only)
        n = 100
        df = gdf(
            n=n,
            n_copies=7,
            seed=1,
            ratio_nans=0.1,
        )
        df_num = df.select_dtypes(include=["number"])

        # transform
        tr = pp.StandardScaler()
        tr = tr.fit(df_num)
        df_transformed = df.copy()
        df_transformed[df_num.columns] = tr.transform(df_num)

        # upload transformer
        db.upload_object(
            engine=self.engine,
            obj=tr,
            table_name="trafo_test",
            date=datetime.datetime.now(),
            symbol="test",
            date_col="date",
            symbol_col="symbol",
        )

        # upload transformer 2
        db.upload_object(
            engine=self.engine,
            obj=tr,
            table_name="trafo_test",
            date=datetime.datetime.now(),
            symbol="test",
            date_col="date",
            symbol_col="symbol",
        )

        # load transformation parameters again
        tr_read = db.get_object(
            engine=self.engine,
            table_name="trafo_test",
            symbol="test",
            date_col="date",
            symbol_col="symbol",
        )

        # test unpickled sklearn object
        self.check_objects(tr, tr_read)

        # sleep
        time.sleep(0.1)


if __name__ == "__main__":
    unittest.main()
