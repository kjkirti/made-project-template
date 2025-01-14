import pandas as pd
import os
from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle_datasets():
    """
    Download Kaggle datasets and return file paths.
    """
    alcohol_dataset = "annafabris/alcohol-consumption-by-state-2024"
    chronic_dataset = "cdc/chronic-disease"
    
    download_dir = "./data"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(alcohol_dataset, path=download_dir, unzip=True)
    api.dataset_download_files(chronic_dataset, path=download_dir, unzip=True)

    alcohol_csv = os.path.join(download_dir, "alcohol_consumption_by_usa_state_2024.csv")
    chronic_csv = os.path.join(download_dir, "U.S._Chronic_Disease_Indicators.csv")
    
    return alcohol_csv, chronic_csv


def load_datasets(alcohol_csv, chronic_csv):
    """
    Load datasets from CSV files and return as DataFrames.
    """
    alcohol = pd.read_csv(alcohol_csv)
    chronic = pd.read_csv(chronic_csv)
    return alcohol, chronic


def preprocess_alcohol_data(alcohol):
    """
    Preprocess the alcohol DataFrame while retaining all columns.
    """
    # Drop rows with missing values in critical columns
    alcohol_cleaned = alcohol.dropna(subset=['Gallons of Ethanol per Capita'])
    # Convert relevant columns to appropriate types (if necessary)
    alcohol_cleaned['Gallons of Ethanol per Capita'] = alcohol_cleaned['Gallons of Ethanol per Capita'].astype(float)
    return alcohol_cleaned


def preprocess_chronic_data(chronic):
    """
    Preprocess the chronic disease DataFrame while retaining all columns.
    """
    # Drop rows with missing values in critical fields
    chronic_cleaned = chronic.dropna(subset=['DataValue'])
    
    # Remove rows with specific footnotes
    chronic_cleaned = chronic_cleaned[~chronic_cleaned['DatavalueFootnote'].isin(
        ["No data available", "Data not shown because of too few respondents or cases"]
    )]
    
    # Fill missing footnotes with an empty string
    chronic_cleaned['DatavalueFootnote'] = chronic_cleaned['DatavalueFootnote'].fillna('')
    
    return chronic_cleaned


def main():
    try:
        # Download Kaggle datasets
        alcohol_csv, chronic_csv = download_kaggle_datasets()
        
        # Load datasets
        alcohol, chronic = load_datasets(alcohol_csv, chronic_csv)
        
        # Preprocess datasets
        alcohol_cleaned = preprocess_alcohol_data(alcohol)
        chronic_cleaned = preprocess_chronic_data(chronic)
        
        # Save the preprocessed datasets
        alcohol_output_csv = './data/alcohol_data.csv'
        chronic_output_csv = './data/chronic_data.csv'

        alcohol_cleaned.to_csv(alcohol_output_csv, index=False, encoding='utf-8-sig')
        chronic_cleaned.to_csv(chronic_output_csv, index=False, encoding='utf-8-sig')

        print(f"Alcohol data cleaned and saved as {alcohol_output_csv}")
        print(f"Chronic disease data cleaned and saved as {chronic_output_csv}")
        
        # Return True to indicate success
        return True
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        return False


if __name__ == "__main__":
    main()

