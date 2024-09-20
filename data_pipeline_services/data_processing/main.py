import logging
import os
import sys

import pandas as pd

from data_pipeline_services.data_processing.cleaning import process_raw_data
from data_pipeline_services.minio_operations import download_csv_from_minio, get_minio_client, list_objects_in_bucket

pd.set_option("future.no_silent_downcasting", True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)


def main():
  try:
    minio_client = get_minio_client()
    bucket_name = os.getenv("MINIO_BUCKET_NAME")

    objects = list_objects_in_bucket(minio_client, bucket_name)
    csv_files = [obj for obj in objects if obj.endswith(".csv")]

    if not csv_files:
      logger.error("No CSV files found in the bucket. Exiting...")
      exit(1)

    latest_file = max(csv_files, key=lambda x: x.split("_")[-1].split(".")[0])
    df = download_csv_from_minio(minio_client, bucket_name, latest_file)

    if df is not None:
      success = process_raw_data(df)
      if not success:
        logger.error("Data processing failed.")
        exit(1)
      logger.info(f"Successfully processed {latest_file}")
      exit(0)
    else:
      logger.error(f"Failed to download {latest_file}")
      exit(1)

  except Exception as e:
    logger.error(f"An error occurred during data processing: {str(e)}")
    exit(1)


if __name__ == "__main__":
  main()
