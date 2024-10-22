# scheduler.py
import schedule
import time
from etl_pipeline import etl_pipeline

def job():
    """
    Defines the ETL job that runs on a schedule.
    """
    input_file = 'data/input_data.csv'
    output_file = 'data/transformed_data.csv'
    etl_pipeline(input_file, output_file)

# Schedule the ETL job to run every day at 1:00 AM
schedule.every().day.at("01:00").do(job)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)
