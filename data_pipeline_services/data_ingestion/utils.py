import unicodedata
import os
import time


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