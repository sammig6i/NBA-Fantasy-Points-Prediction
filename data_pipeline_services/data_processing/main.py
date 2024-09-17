import logging
import os
import time
import sys

import pandas as pd
from cleaning import process_raw_data
from dotenv import load_dotenv
from minio_operations import download_csv_from_minio, get_minio_client, list_objects_in_bucket

pd.set_option("future.no_silent_downcasting", True)

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)


def main():
  minio_client = get_minio_client()
  bucket_name = os.getenv("MINIO_BUCKET_NAME")

  max_retries = 30
  retry_delay = 10

  for attempt in range(max_retries):
    if os.path.exists("/tmp/data_ingestion_complete"):
      break
    elif attempt < max_retries - 1:
      logger.info(f"Waiting for data ingestion to complete. Retrying in {retry_delay} seconds...")
      time.sleep(retry_delay)
    else:
      logger.error("Data ingestion did not complete after all retries. Exiting...")
      return

  objects = list_objects_in_bucket(minio_client, bucket_name)
  csv_files = [obj for obj in objects if obj.endswith(".csv")]

  if not csv_files:
    logger.error("No CSV files found in the bucket. Exiting...")
    return

  latest_file = max(csv_files, key=lambda x: x.split("_")[-1].split(".")[0])
  df = download_csv_from_minio(minio_client, bucket_name, latest_file)

  if df is not None:
    try:
      process_raw_data(df)
      logger.info(f"Successfully processed {latest_file}")
    except Exception as e:
      logger.error(f"Error processing {latest_file}: {str(e)}")
  else:
    logger.error(f"Failed to download {latest_file}")


if __name__ == "__main__":
  main()
