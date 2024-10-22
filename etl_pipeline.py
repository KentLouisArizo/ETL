# etl_pipeline.py
from etl_functions import extract, transform, load

def etl_pipeline(input_file, output_file):
    """
    Orchestrates the ETL process.
    :param input_file: str - Path to the input CSV file
    :param output_file: str - Path to the output CSV file
    """
    # Step 1: Extract data
    data = extract(input_file)
    
    # Step 2: Transform data
    transformed_data = transform(data)
    
    # Step 3: Load transformed data
    load(transformed_data, output_file)

if __name__ == "__main__":
    # Define input and output paths
    input_file = 'data/input_data.csv'
    output_file = 'data/transformed_data.csv'
    
    # Run the ETL pipeline
    etl_pipeline(input_file, output_file)
