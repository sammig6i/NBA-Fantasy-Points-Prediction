# Entry point to core scraper for player box score stats
import logging
import os
import sys
from datetime import datetime

import yaml

from data_pipeline_services.data_ingestion.scraper import extract_player_data, get_box_score_links, get_month_links
from data_pipeline_services.data_ingestion.utils import adjust_dates_based_on_season
from data_pipeline_services.minio_operations import get_minio_client, upload_to_minio

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)


def main():
  try:
    yaml_file = "/app/data_pipeline_services/config/scraping_config.yml"

    with open(yaml_file, "r") as file:
      config = yaml.safe_load(file)

    scraper = config["scraping_job"]
    season = scraper["season"]
    input_start_date = scraper["start_date"]
    input_end_date = scraper["end_date"]

    default_start = config["default_nba_dates"]["start"]
    default_end = config["default_nba_dates"]["end"]

    result = get_month_links(season)
    if result is None:
      logger.error("Error getting month links. Exiting...")
      exit(1)

    month_links, start_year, end_year = result

    if not input_start_date or not input_end_date:
      start_date, end_date = adjust_dates_based_on_season(start_year, end_year, default_start, default_end)
    else:
      start_date, end_date = adjust_dates_based_on_season(start_year, end_year, input_start_date, input_end_date)

    box_score_links, all_dates = get_box_score_links(month_links, start_date, end_date, start_year, end_year)
    if box_score_links is None or all_dates is None:
      logger.error("Error getting box score links. Exiting...")
      exit(1)

    df = extract_player_data(box_score_links, all_dates)
    if df.empty:
      logger.error("No data extracted. DataFrame is empty. Exiting...")
      exit(1)

    minio_config = config["minio"]
    bucket_name = os.getenv("MINIO_BUCKET_NAME")

    base_output_dir = minio_config["output_dir"]
    output_dir = f"{base_output_dir}/{season}"
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    object_name = f"{output_dir}/nba_player_stats_{season}_{start_date}_to_{end_date}_{current_timestamp}.csv"

    try:
      minio_client = get_minio_client()
      upload_to_minio(minio_client, df, bucket_name, object_name)
      logger.info(f"Data successfully uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
    except Exception as e:
      logger.error(f"Error uploading data to MinIO: {e}")
      exit(1)

    logger.info("Data ingestion completed successfully")
    return True
  except Exception as e:
    logger.error(f"Error in data ingestion: {str(e)}")
    return False


if __name__ == "__main__":
  main()
