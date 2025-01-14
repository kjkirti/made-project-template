import unittest
import os
import pandas as pd
import sqlite3
from pipeline import main


class TestDataPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up the test environment by running the pipeline.
        This ensures output files are generated before running tests.
        """
        print("Running data pipeline for testing...")
        main()

    def test_csv_file_exists(self):
        """
        Test if the output CSV files exist.
        """
        alcohol_file = "./data/alcohol_data.csv"
        chronic_file = "./data/chronic_data.csv"
        self.assertTrue(os.path.isfile(alcohol_file), f"{alcohol_file} does not exist")
        self.assertTrue(os.path.isfile(chronic_file), f"{chronic_file} does not exist")

    def test_csv_file_content(self):
        """
        Test if the output CSV files are not empty and contain data.
        """
        alcohol_file = "./data/alcohol_data.csv"
        chronic_file = "./data/chronic_data.csv"

        alcohol_df = pd.read_csv(alcohol_file, low_memory=False)
        chronic_df = pd.read_csv(chronic_file, low_memory=False)

        self.assertFalse(alcohol_df.empty, f"{alcohol_file} is empty")
        self.assertFalse(chronic_df.empty, f"{chronic_file} is empty")
        self.assertTrue(len(alcohol_df) > 0, f"{alcohol_file} has no data rows")
        self.assertTrue(len(chronic_df) > 0, f"{chronic_file} has no data rows")


    def test_pipeline_output(self):
        """
        Test the pipeline's main function output.
        """
        result = main()
        self.assertTrue(result, "Pipeline execution failed")


if __name__ == "__main__":
    unittest.main()
