import io
import os

import pandas as pd
from dotenv import load_dotenv

from minio import Minio

load_dotenv()


def get_minio_client() -> Minio:
  return Minio(
    endpoint=os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
  )


def list_objects_in_bucket(minio_client: Minio, bucket_name: str) -> list[str]:
  objects = minio_client.list_objects(bucket_name, recursive=True)
  return [obj.object_name for obj in objects]


def download_csv_from_minio(minio_client: Minio, bucket_name: str, object_name: str) -> pd.DataFrame | None:
  try:
    response = minio_client.get_object(bucket_name, object_name)
    return pd.read_csv(io.BytesIO(response.read()))
  except Exception as e:
    print(f"Error downloading {object_name}: {e}")
    return None


def upload_to_minio(minio_client: Minio, df: pd.DataFrame, bucket_name: str, object_name: str) -> None:
  try:
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    if not minio_client.bucket_exists(bucket_name):
      minio_client.make_bucket(bucket_name)
    minio_client.put_object(
      bucket_name,
      object_name,
      io.BytesIO(csv_buffer.getvalue().encode("utf-8")),
      len(csv_buffer.getvalue().encode("utf-8")),
      content_type="text/csv",
    )
    print(f"File {object_name} successfully uploaded to bucket {bucket_name}")
  except Exception as e:
    print(f"Failed to upload {object_name}: {str(e)}")
