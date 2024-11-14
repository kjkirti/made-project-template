import os
import pandas as pd
import requests
from zipfile import ZipFile
from io import BytesIO

# Define the URLs for the datasets
dataset_urls = [
    "https://www.kaggle.com/datasets/mathchi/diabetes-data-set",
    "https://www.kaggle.com/datasets/cdc/chronic-disease"
]

# Directory to save data
output_dir = './data'
os.makedirs(output_dir, exist_ok=True)

def download_and_extract_kaggle_dataset(url, output_directory):
    # This is a placeholder for Kaggle dataset download.
    print(f"Please manually download the dataset from {url} due to API restrictions.")
    print("Place the unzipped files in the output directory specified.")

def clean_and_transform_data(file_path):
    # Load CSV/Excel file
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith('.xlsx') or file_path.endswith('.xls'):
        df = pd.read_excel(file_path)
    else:
        print(f"Unsupported file format: {file_path}")
        return None

    # Basic data cleaning
    df.columns = df.columns.str.strip()  # Strip leading/trailing spaces from column names
    df = df.dropna()  # Drop rows with missing values

    # Example transformation: Remove duplicate rows
    df = df.drop_duplicates()

    # Example transformation: Normalize column names to lowercase
    df.columns = df.columns.str.lower()

    return df

# Placeholder: Replace with paths to your locally downloaded files
dataset_paths = [
    './data/diabetes.csv',  # Path to downloaded and extracted diabetes data
    './data/chronic_disease.csv'  # Path to downloaded and extracted chronic disease data
]

# Process each dataset
for path in dataset_paths:
    if os.path.exists(path):
        df = clean_and_transform_data(path)
        if df is not None:
            output_file = os.path.join(output_dir, os.path.basename(path))
            df.to_csv(output_file, index=False)
            print(f"Cleaned data saved to {output_file}")
    else:
        print(f"File {path} not found. Please download it manually from the specified Kaggle link.")

print("Data processing complete.")
