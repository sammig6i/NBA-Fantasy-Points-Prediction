from minio_operations import get_minio_client, list_objects_in_bucket, download_csv_from_minio
from .cleaning import process_raw_data
from dotenv import load_dotenv
import os

load_dotenv()

def main():
  minio_client = get_minio_client()
  bucket_name = os.getenv('MINIO_BUCKET_NAME')

  objects = list_objects_in_bucket(minio_client, bucket_name)

  for object_name in objects:
    if object_name.endswith('.csv'):
      print(f"Processing {object_name}")
      df = download_csv_from_minio(minio_client, bucket_name, object_name)
        
      if df is not None:
        process_raw_data(df)
      else:
        print(f"Skipping {object_name} due to download error")

if __name__ == "__main__":
    main()

