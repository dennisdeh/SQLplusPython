import unittest
import os
import sqlpluspython.db_connection as db
import sqlalchemy
from sqlpluspython.testing.aa_cleaner.clean_test_database import reset_test_tables

path_db_env = "./.env"


class ConnectionToSQLDatabase(unittest.TestCase):
    """
    Tests for basic interacting with the database MariaDB SQL backend
    """

    def test0_load_env_variables_exception(self):
        # 1: Check exception
        self.assertRaises(FileNotFoundError, db.load_env_variables, "SOME INVALID PATH")

    def test1_load_env_variables(self):
        # 1: Load env variables
        self.assertIsNone(db.load_env_variables(path=path_db_env))
        # check that env variables are now set
        self.assertFalse(os.getenv("MARIADB_USER") == "")
        self.assertFalse(os.getenv("MARIADB_PASSWORD") == "")
        self.assertFalse(os.getenv("DB_PORT_HOST") == "")

        # 2: check using function in the module
        self.assertTrue(db.check_env_variables_loaded())

    def test2_connection_string(self):
        # 1: Load env variables
        out = db.get_connection_string("A_TEST_DATABASE")
        self.assertIsInstance(out, str)
        self.assertTrue(out.find("A_TEST_DATABASE") >= 0)

    def test3_get_engine_check_database(self):
        # 0: check expected exceptions
        self.assertRaises(AssertionError, db.get_engine, ["LISTS ARE INVALID"])
        # 1: check that invalid database is not available
        engine = db.get_engine("INVALID DATABASE")
        self.assertFalse(db.check_database_available(engine))
        # 2: checks that a valid database is available
        engine = db.get_engine("testing")
        self.assertTrue(db.check_database_available(engine))

    def test4_execute_queries_truncate_tables(self):
        """
        Execute queries against the database which
        deletes all rows of all tables in the testing
        database
        """
        # 0: (re)load environment variables, get engine
        db.load_env_variables(path=path_db_env)
        engine = db.get_engine("testing")

        # 1: delete all rows in all tables in "testing" database
        with engine.connect() as connection:
            md = sqlalchemy.MetaData()
            md.reflect(bind=engine)
            for table in md.tables:
                query = f"TRUNCATE TABLE `{table}`;"
                # execute query
                out = connection.execute(sqlalchemy.text(query))
                self.assertIsNone(out.cursor)

        # 2: verify that all tables are empty
        with engine.connect() as connection:
            md = sqlalchemy.MetaData()
            md.reflect(bind=engine)
            for table in md.tables:
                # parse query
                query = f"SELECT COUNT(*) FROM `{table}`;"
                # execute query
                result_proxy = connection.execute(sqlalchemy.text(query))
                # fetch the result
                result = result_proxy.fetchall()
                self.assertEqual(result[0][0], 0)

        # 3: delete everything to reset
        reset_test_tables(engine=engine, confirm=True)
        engine.dispose(close=True)


if __name__ == "__main__":
    unittest.main()
