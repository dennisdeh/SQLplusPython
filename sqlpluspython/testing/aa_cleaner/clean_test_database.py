from typing import Union

import sqlalchemy


def reset_test_tables(
    engine,
    database: str = "testing",
    tables_standard: Union[None, list] = None,
    confirm=False,
):
    """
    Delete all non-standard tables in the testing database
    and truncate the standard tables
    """
    if confirm:
        if tables_standard is None:
            tables_standard = [
                "Prices",
                "Meta data",
                "Income",
                "Balance",
                "Cash flow",
                "Financial ratios",
                "Enterprise value",
                "Earnings",
                "FMP rating",
                "Key metrics",
                "News",
                "Dividends",
                "Earnings Call Transcript",
                "Insider transactions",
            ]
        elif isinstance(tables_standard, list):
            pass
        else:
            raise ValueError("tables_standard must be a list or None")

        # delete all rows in all tables in the database
        with engine.connect() as connection:
            md = sqlalchemy.MetaData()
            md.reflect(bind=engine)
            for table in md.tables:
                if table in tables_standard:
                    print(f"Truncating table: {table}")
                    query = f"TRUNCATE TABLE `{table}`;"
                    # execute query
                    out = connection.execute(sqlalchemy.text(query))
                else:
                    print(f"Dropping table: {table}")
                    query = f"DROP TABLE `{table}`;"
                    # execute query
                    out = connection.execute(sqlalchemy.text(query))

        # verify that tables are all empty or deleted
        with engine.connect() as connection:
            md = sqlalchemy.MetaData()
            md.reflect(bind=engine)
            for table in md.tables:
                if table in tables_standard:
                    # parse query
                    query = f"SELECT COUNT(*) FROM `{table}`;"
                    # execute query
                    result_proxy = connection.execute(sqlalchemy.text(query))
                    # fetch the result
                    result = result_proxy.fetchall()
                    assert (
                        result[0][0] == 0
                    ), "Error: testing database is not empty, when it is expected to be!"
                else:
                    # parse query
                    query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{database}' AND table_name = '{table}';"
                    # execute query
                    result_proxy = connection.execute(sqlalchemy.text(query))
                    # fetch the result
                    result = result_proxy.fetchall()
                    assert (
                        result[0][0] == 0
                    ), "Error: table exists, when it is expected to!"

        # dispose of connections
        engine.dispose(close=True)
    else:
        raise ValueError("Confirmation not given")


def clean_test_database(engine):
    """
    Truncate all tables in the database associated with the given engine
    """
    # delete all rows in all tables in the database
    with engine.connect() as connection:
        md = sqlalchemy.MetaData()
        md.reflect(bind=engine)
        for table in md.tables:
            print(f"Truncating table: {table}")
            query = f"TRUNCATE TABLE `{table}`;"
            # execute query
            out = connection.execute(sqlalchemy.text(query))
    # verify that tables are all empty
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
            assert (
                result[0][0] == 0
            ), "Error: database is not empty, when it is expected to be!"
    # dispose of connections
    engine.dispose(close=True)
