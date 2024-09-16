import debugpy

print("Ready to attach debugger. Start your debugger now.")
debugpy.listen(("0.0.0.0", 5678))
debugpy.wait_for_client()
print("Debugger attached")

from cleaning import process_raw_data
from minio_operations import get_minio_client, list_objects_in_bucket, download_csv_from_minio
from dotenv import load_dotenv
import os
import logging
import time
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
  minio_client = get_minio_client()
  bucket_name = os.getenv('MINIO_BUCKET_NAME')

  max_retries = 30  # Increased retries
  retry_delay = 10  # Shorter delay

  for attempt in range(max_retries):
    objects = list_objects_in_bucket(minio_client, bucket_name)
    csv_files = [obj for obj in objects if obj.endswith('.csv')]

    if csv_files:
      break
    elif attempt < max_retries - 1:
      logger.info(f"No CSV files found in the bucket. Retrying in {retry_delay} seconds...")
      time.sleep(retry_delay)
    else:
      logger.error("No CSV files found in the bucket after all retries. Exiting...")
      return

  latest_file = max(csv_files, key=lambda x: x.split('_')[-1].split('.')[0])
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

