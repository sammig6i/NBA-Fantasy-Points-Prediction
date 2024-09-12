from minio import Minio
from dotenv import load_dotenv
import os
import io
import pandas as pd
import yaml

load_dotenv()

def get_minio_client():
  current_dir = os.path.dirname(os.path.abspath(__file__))
  yaml_file = os.path.join(current_dir, 'config', 'scraping_config.yml')

  with open(yaml_file, 'r') as file:
    config = yaml.safe_load(file)

  minio_config = config['minio']
  access_key = os.getenv(minio_config['access_key'])
  secret_key = os.getenv(minio_config['secret_key'])

  return Minio(
    minio_config['endpoint'],
    access_key=access_key,
    secret_key=secret_key,
    secure=minio_config['secure']
  )

def upload_to_minio(df: pd.DataFrame, bucket_name: str, object_name: str) -> None:
  """
  Upload a Dataframe to MinIO as a CSV from memory
  """
  minio_client = get_minio_client()
  try:
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    if not minio_client.bucket_exists(bucket_name):
      minio_client.make_bucket(bucket_name)
    minio_client.put_object(
      bucket_name, 
      object_name, 
      io.BytesIO(csv_buffer.getvalue().encode('utf-8')), 
      len(csv_buffer.getvalue().encode('utf-8')), 
      content_type='text/csv'
    )
    print(f"File {object_name} successfully uploaded to bucket {bucket_name}")
  except Exception as e:
    print(f"Failed to upload {object_name}: {str(e)}")
