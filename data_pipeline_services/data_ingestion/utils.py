import calendar
from typing import List, Tuple
import unicodedata
import time
from datetime import datetime
from data_pipeline_services.config.common.variables import MONTH_START_END_DATES


def normalize_name(name):
  """Normalize player names by removing diacritics and converting to lowercase."""
  normalized = unicodedata.normalize('NFKD', name)
  without_diacritics = ''.join(c for c in normalized if not unicodedata.combining(c))
  return without_diacritics.lower()


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
  print(f"Error occurred: {link}: {str(error)}")


def apply_year_to_months(start_year_full: int, end_year_full: int) -> dict:
  """ 
  Dynamically apply the correct year to year-agnostic months, 
  and handle leap years for February.
  """
  month_start_end_dates_with_years = {}

  for month, (month_start, month_end) in MONTH_START_END_DATES.items():
    if month in ['september','october', 'november', 'december']:  # Belongs to the start year
      year = start_year_full
    else:
      year = end_year_full

    if month == 'february' and calendar.isleap(year):
      month_end = '02-29'

    full_month_start = f"{year}-{month_start}"
    full_month_end = f"{year}-{month_end}"

    month_start_end_dates_with_years[month] = (full_month_start, full_month_end)

  return month_start_end_dates_with_years


def filter_relevant_months(month_link_list: List[Tuple[str, str]], 
                           start_date_dt: datetime,
                           end_date_dt: datetime, 
                           start_year_full: int, 
                           end_year_full: int) -> List[Tuple[str, str]]:
  """
  Filter out irrelevant months based on the start and end dates.
  """
  relevant_month_links = []
  month_start_end_dates_with_years = apply_year_to_months(start_year_full, end_year_full)

  for month, link in month_link_list:
    month_start_str, month_end_str = month_start_end_dates_with_years.get(month, (None, None))
    if month_start_str and month_end_str:
      month_start_dt = datetime.strptime(month_start_str, '%Y-%m-%d')
      month_end_dt = datetime.strptime(month_end_str, '%Y-%m-%d')
        
      if month_end_dt >= start_date_dt and month_start_dt <= end_date_dt:
        relevant_month_links.append((month, link))
    
  return relevant_month_links


def adjust_dates_based_on_season(start_year_full: int, end_year_full: int, start_date: str, end_date: str) -> Tuple[str, str]:
  """
  Adjust the start and end dates based on the season.
  Assigns the correct year to the dates provided based on the month.
  """
  start_month = int(start_date.split("-")[0])
  end_month = int(end_date.split("-")[0])
  
  if start_month >= 10:  # October - December
    adjusted_start_date = f"{start_year_full}-{start_date}"
  else:
    adjusted_start_date = f"{end_year_full}-{start_date}"
  
  if end_month >= 10:  
    adjusted_end_date = f"{start_year_full}-{end_date}"
  else:
    adjusted_end_date = f"{end_year_full}-{end_date}"
  
  return adjusted_start_date, adjusted_end_date