import calendar
from typing import List, Tuple
import unicodedata
import os
import time
from datetime import datetime
from .config.variables import MONTH_START_END_DATES


def normalize_name(name):
  """Normalize player names by removing diacritics and converting to lowercase."""
  normalized = unicodedata.normalize('NFKD', name)
  without_diacritics = ''.join(c for c in normalized if not unicodedata.combining(c))
  return without_diacritics.lower()


def save_dataframes_to_csv(df, season, output_dir="output"):
  if not os.path.exists(output_dir):
    os.makedirs(output_dir)

  file_name = f"{season}_Season.csv"
  file_path = os.path.join(output_dir, file_name)

  df.to_csv(file_path, lineterminator='\n', index=False)
  print(f'Saved game stats for the {season} season to {file_name}')


def handle_http_error(response):
  """Handle HTTP errors."""
  if response.status_code == 429:
    retry_after = response.headers.get('Retry-After', None)
    print(f"Rate limit exceeded. Status code: {response.status_code}. Retry-After: {retry_after}")
    if retry_after:
      time.sleep(int(retry_after))
  else:
    print(f"HTTP error occurred: {response.status_code} - {response.reason}")
  return None


def handle_general_error(error, link):
  """Handle general errors."""
  print(f"Error occurred for {link}: {str(error)}")


def validate_date_format(date_str: str, year: int) -> datetime:
  """
    Validate date format and return datetime.
  """
  try:
    return datetime.strptime(f"{year}-{date_str}", '%Y-%m-%d')
  except ValueError:
    raise ValueError(f"Invalid date format: {date_str}. Expected format: YYYY-MM-DD.")
  

def apply_year_to_months(start_year: int, end_year: int) -> dict:
  """ 
  Dynamically apply the correct year to year-agnostic months, 
  and handle leap years for February.
  """
  month_start_end_dates_with_years = {}

  for month, (month_start, month_end) in MONTH_START_END_DATES.items():
    if month in ['september','october', 'november', 'december']:  # Belongs to the start year
      year = start_year
    else:
      year = end_year

    if month == 'february' and calendar.isleap(year):
      month_end = '02-29'

    full_month_start = f"{year}-{month_start}"
    full_month_end = f"{year}-{month_end}"

    month_start_end_dates_with_years[month] = (full_month_start, full_month_end)

  return month_start_end_dates_with_years


def filter_relevant_months(month_link_list: List[Tuple[str, str]], 
                           start_date_dt: datetime,
                           end_date_dt: datetime, 
                           start_year: int, 
                           end_year: int) -> List[Tuple[str, str]]:
  """
  Filter out irrelevant months based on the start and end dates.
  """
  relevant_month_links = []
  month_start_end_dates_with_years = apply_year_to_months(start_year, end_year)

  for month, link in month_link_list:
    month_start_str, month_end_str = month_start_end_dates_with_years.get(month, (None, None))
    if month_start_str and month_end_str:
      month_start_dt = datetime.strptime(month_start_str, '%Y-%m-%d')
      month_end_dt = datetime.strptime(month_end_str, '%Y-%m-%d')
        
      if month_end_dt >= start_date_dt and month_start_dt <= end_date_dt:
        relevant_month_links.append((month, link))
    
  return relevant_month_links