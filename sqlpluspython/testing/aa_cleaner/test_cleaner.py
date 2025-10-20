import unittest
import shutil
import os
import sqlpluspython.p00_databases.db_connection as db
from sqlpluspython.utils.paths import get_project_path, delete_files_extension
from sqlpluspython.testing.aa_cleaner.clean_test_database import reset_test_tables


path_db_env = "./modules/p00_databases/.env"

import os


def helper_delete_folders(
    path: str, starting_with: str, crawl_subfolders: bool, confirm: bool
):
    """
    Deletes all folders in the specified path that start with the
    string starting_with.

    Parameters
    ----------
    path (str): The directory path to scan for folders
    starting_with (str): The beginning of the name of the folders to delete
    crawl_subfolders (bool): If True, all subfolders with that name will also be deleted
    confirm (bool): Confirmation must be given, otherwise nothing will be deleted
    """
    assert isinstance(confirm, bool), "confirm must be bool"
    assert confirm, "confirmation must be given"
    assert isinstance(path, str)
    assert isinstance(starting_with, str)
    assert isinstance(crawl_subfolders, bool)
    assert len(path) > 0
    assert len(starting_with) > 0

    if not os.path.exists(path):
        raise OSError(f"Path '{path}' does not exist.")

    # Iterate through items in the directory
    if crawl_subfolders:
        for dirpath, dirnames, _ in os.walk(path, topdown=False):
            for dirname in dirnames:
                item_path = os.path.join(dirpath, dirname)
                # Check if the item is a directory and starts with 'A'
                if os.path.isdir(item_path) and dirname.startswith(starting_with):
                    try:
                        # Remove the directory
                        shutil.rmtree(item_path)
                        print(f"Deleted folder: {item_path}")
                    except OSError as e:
                        print(f"Failed to delete folder '{item_path}': {e}")
                else:
                    pass
    else:
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            # Check if the item is a directory and starts with 'A'
            if os.path.isdir(item_path) and item.startswith(starting_with):
                try:
                    # Remove the directory
                    shutil.rmtree(item_path)
                    print(f"Deleted folder: {item_path}")
                except OSError as e:
                    print(f"Failed to delete folder '{item_path}': {e}")
            else:
                pass


class TestCleaner(unittest.TestCase):
    """
    Delete temporary folders and files
    """

    # initialisation
    assert (
        get_project_path("Investio") == os.getcwd()
        or "/mnt/Data/Documents/Investio" == os.getcwd()
    ), "The current working directory must be the project directory"

    def test_delete_folders(self):
        # 1: delete folders directly specified
        # expected folders to delete (all paths relative to the project root)
        list_dir = [
            "modules/testing/testdata/0/combined_data",
            "modules/testing/testdata/0_av/combined_data",
            "modules/testing/testdata/0_fmp/combined_data",
            "modules/testing/testdata/p02_data/outputs",
            "modules/testing/testdata/p03_data/macrodata_temp",
            "modules/testing/testdata/p04_data/SPP_weekly/testing_new_will_be_deleted",
            "modules/testing/testdata/p04_data/SPP_daily/testing_new_will_be_deleted",
            "modules/testing/testdata/p04_data/stock_data/combined_data",
        ]
        for rel_dir in list_dir:
            dir = os.path.join(get_project_path("Investio"), rel_dir)
            shutil.rmtree(dir)
            # create new empty dir
            os.makedirs(dir)
            self.assertTrue(os.path.isdir(dir))

        # 2: delete all folders following a special pattern
        helper_delete_folders(
            os.path.join(
                get_project_path("Investio"),
                "modules/testing/testdata/p05_data/spp_test_data_to_load_Excel",
            ),
            starting_with="STRGY_strategy_test_20",
            crawl_subfolders=False,
            confirm=True,
        )
        helper_delete_folders(
            os.path.join(
                get_project_path("Investio"),
                "modules/testing/testdata/p05_data/spp_test_data_to_load_Excel",
            ),
            starting_with="STRGY_strategy_adv_test_20",
            crawl_subfolders=False,
            confirm=True,
        )
        helper_delete_folders(
            os.path.join(
                get_project_path("Investio"),
                "modules/testing/testdata/p05_data/spp_test_data_to_load_SQL",
            ),
            starting_with="STRGY_strategy_test_20",
            crawl_subfolders=False,
            confirm=True,
        )
        helper_delete_folders(
            os.path.join(
                get_project_path("Investio"),
                "modules/testing/testdata/p05_data/spp_test_data_to_load_SQL",
            ),
            starting_with="STRGY_strategy_adv_test_20",
            crawl_subfolders=False,
            confirm=True,
        )

        # 3: delete all subfolders in folders following a special pattern
        helper_delete_folders(
            os.path.join(
                get_project_path("Investio"),
                "modules/testing/testdata/p05_data/spp_test_data_to_load_Excel",
            ),
            starting_with="OPT_20",
            crawl_subfolders=True,
            confirm=True,
        )
        helper_delete_folders(
            os.path.join(
                get_project_path("Investio"),
                "modules/testing/testdata/p05_data/spp_test_data_to_load_Excel",
            ),
            starting_with="old_forecast_20",
            crawl_subfolders=True,
            confirm=True,
        )
        helper_delete_folders(
            os.path.join(
                get_project_path("Investio"),
                "modules/testing/testdata/p05_data/spp_test_data_to_load_SQL",
            ),
            starting_with="OPT_20",
            crawl_subfolders=True,
            confirm=True,
        )
        helper_delete_folders(
            os.path.join(
                get_project_path("Investio"),
                "modules/testing/testdata/p05_data/spp_test_data_to_load_SQL",
            ),
            starting_with="old_forecast_20",
            crawl_subfolders=True,
            confirm=True,
        )
        helper_delete_folders(
            path="modules/testing/testdata/p04_data/SPP_weekly/testing_data_to_load",
            starting_with="hyperparameter_optimisation_20",
            crawl_subfolders=False,
            confirm=True,
        )

    def test_reset_testing_database(self):
        """
        Delete non-standard tables and truncate standard tables
        in the testing database
        """
        db.load_env_variables(path=path_db_env)
        engine = db.get_engine("testing")
        reset_test_tables(engine=engine, confirm=True)
        self.assertTrue(True)
