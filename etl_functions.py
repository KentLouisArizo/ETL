# etl_functions.py
import pandas as pd
import logging

# Configure logging
logging.basicConfig(filename='etl_log.log', level=logging.INFO)

def extract(file_path):
    """
    Reads a CSV file and loads it into a DataFrame.
    :param file_path: str - The path to the CSV file
    :return: DataFrame
    """
    try:
        data = pd.read_csv(file_path)
        logging.info(f"Data successfully loaded from {file_path}.")
        return data
    except FileNotFoundError as e:
        logging.error(f"Error loading file: {e}")
        return None

def transform(data):
    """
    Transforms the data (e.g., cleaning, renaming, or filtering).
    :param data: DataFrame
    :return: DataFrame
    """
    if data is None:
        logging.error("No data to transform.")
        return None
    
    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    data_cleaned = data.dropna(subset=['age']).copy()
    logging.info("Rows with missing 'age' dropped.")

    # Example: renaming a column
    if 'old_column_name' in data_cleaned.columns:
        data_cleaned.rename(columns={'old_column_name': 'new_column_name'}, inplace=True)
        logging.info("Renamed column 'old_column_name' to 'new_column_name'.")
    
    # Example: converting date column to datetime
    if 'date_column' in data_cleaned.columns:
        data_cleaned['date_column'] = pd.to_datetime(data_cleaned['date_column'])
        logging.info("Converted 'date_column' to datetime format.")

    return data_cleaned

def load(data, output_file):
    """
    Saves the transformed data to a CSV file.
    :param data: DataFrame
    :param output_file: str
    """
    if data is None:
        logging.error("No data to load.")
        return
    
    try:
        data.to_csv(output_file, index=False)
        logging.info(f"Data successfully saved to {output_file}.")
    except Exception as e:
        logging.error(f"Error saving file: {e}")
