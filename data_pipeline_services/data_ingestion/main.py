# Entry point to core scraper for player box score stats

from scraper import get_month_links, get_box_score_links, extract_player_data
from utils import adjust_dates_based_on_season
from minio_operations import upload_to_minio, get_minio_client
from datetime import datetime
import yaml
import os
from datetime import datetime

def main():
  yaml_file = '/app/data_pipeline_services/config/scraping_config.yml'

  with open(yaml_file, 'r') as file:
    config = yaml.safe_load(file)

  scraper = config['scraping_job']
  season = scraper['season']
  input_start_date = scraper['start_date']
  input_end_date = scraper['end_date']

  default_start = config['default_nba_dates']['start']
  default_end = config['default_nba_dates']['end']

  result = get_month_links(season)
  if result is None:
    print("Error getting month links. Exiting...")
    exit(1)

  month_links, start_year, end_year = result

  if not input_start_date or not input_end_date:
    start_date, end_date = adjust_dates_based_on_season(start_year, end_year, default_start, default_end)
  else:
    start_date, end_date = adjust_dates_based_on_season(start_year, end_year, input_start_date, input_end_date)

  box_score_links, all_dates = get_box_score_links(month_links, start_date, end_date, start_year, end_year)
  if box_score_links is None or all_dates is None:
    print("Error getting box score links. Exiting...")
    exit(1)

  df = extract_player_data(box_score_links, all_dates)
  if df.empty:
    print("No data extracted. DataFrame is empty. Exiting...")
    exit(1)

  minio_config = config['minio']
  bucket_name = os.getenv('MINIO_BUCKET_NAME')

  base_output_dir = minio_config['output_dir']
  output_dir = f"{base_output_dir}/{season}"
  current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  object_name = f"{output_dir}/nba_player_stats_{season}_{start_date}_to_{end_date}_{current_timestamp}.csv"

  try:
    minio_client = get_minio_client()
    upload_to_minio(minio_client, df, bucket_name, object_name)
    print(f"Data successfully uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
  except Exception as e:
    print(f"Error uploading data to MinIO: {e}")
    exit(1)

  print("Data ingestion completed successfully")
  exit(0)

if __name__ == "__main__":
  main()
