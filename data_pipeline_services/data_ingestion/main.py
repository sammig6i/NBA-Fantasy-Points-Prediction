# Entry point to core scraper for box score stats

from .scraper import get_month_links, get_box_score_links, extract_player_data
from .utils import adjust_dates_based_on_season
import yaml
import os

if __name__ == "__main__":
  current_dir = os.path.dirname(os.path.abspath(__file__))
  yaml_file = os.path.join(current_dir, 'config', 'scraping_config.yml')

  with open(yaml_file, 'r') as file:
    config = yaml.safe_load(file)

  scraper = config['scraping_job']
  season = scraper['season']
  input_start_date = scraper['start_date']
  input_end_date = scraper['end_date']

  default_start = config['default_nba_dates']['start']
  default_end = config['default_nba_dates']['end']

  month_links, start_year, end_year = get_month_links(season)

  if not input_start_date or not input_end_date:
    start_date, end_date = adjust_dates_based_on_season(start_year, end_year, default_start, default_end)
  else:
    start_date, end_date = adjust_dates_based_on_season(start_year, end_year, input_start_date, input_end_date)

  box_score_links, all_dates = get_box_score_links(month_links, start_date, end_date, start_year, end_year)

  df = extract_player_data(box_score_links, all_dates)
  print(df.head())