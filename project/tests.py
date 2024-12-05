import unittest
import warnings
from ETL import extract, transform, load
from pipeline import main

class TestExtractor(unittest.TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.api_url_unemployment = "https://api.worldbank.org/v2/en/indicator/SL.UEM.TOTL.ZS?downloadformat=csv"
        self.api_url_crime = "https://api.worldbank.org/v2/en/indicator/VC.IHR.PSRC.P5?downloadformat=csv"
        self.download_path = r"D:\Github\made_ws24\data"  # Corrected raw string

    def test_extract_unemployment_data(self):
        result = extract.Extractor.extract_unemployment_data(self.api_url_unemployment, self.download_path)
        self.assertTrue(result, "Fail")

    def test_extract_crime_data(self):
        result = extract.Extractor.extract_crime_data(self.api_url_crime, self.download_path)
        self.assertTrue(result, "Fail")

class TestTransformer(unittest.TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.t = transform.Transformer()

    def test_null_values(self):
        # Testing null values
        self.t.transform_data_delete_null()
        self.assertFalse(self.t.get_unemployment_data_not_null().isnull().values.any())
        self.assertFalse(self.t.get_crime_data_not_null().isnull().values.any())

    def test_transform_data(self):
        self.t.sync_both_data()
        # Testing columns of both dataframes are equal
        self.assertTrue(
            self.t.get_unemployment_data().columns.equals(self.t.get_crime_data().columns),
            "Columns of both dataframes are not equal"
        )
        # Testing rows of both dataframes are equal
        self.assertTrue(
            self.t.get_unemployment_data().T.columns.equals(self.t.get_crime_data().T.columns),
            "Rows of both dataframes are not equal"
        )

class TestLoader(unittest.TestCase):
    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.t = transform.Transformer()
        self.t.transform_data_delete_null()
        self.t.sync_both_data()
        self.output_file_u = r"D:\Github\made_ws24\data\unemployment.csv"  # Corrected raw string
        self.output_file_c = r"D:\Github\made_ws24\data\crime.csv"  # Corrected raw string

    def test_load_data_and_save(self):
        resultu = load.Loader().load_data_and_save(self.t.get_unemployment_data(), self.output_file_u)
        self.assertTrue(resultu, "Fail")
        resultc = load.Loader().load_data_and_save(self.t.get_crime_data(), self.output_file_c)
        self.assertTrue(resultc, "Fail")

class TestPipeline(unittest.TestCase):
    def test_main(self):
        m = main()
        self.assertTrue(m, "Everything is not fine")

if __name__ == '__main__':
    warnings.filterwarnings("ignore")
    unittest.main()