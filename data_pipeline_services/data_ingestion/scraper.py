from datetime import datetime
from typing import List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import random
from .utils import normalize_name, save_dataframes_to_csv, handle_http_error, handle_general_error
from .config.variables import BASE_URL, MONTH_DICT, TEAM_ABBREVIATIONS
from minio import Minio
from dotenv import load_dotenv
import os
import io

load_dotenv()

minio_client = Minio(
  "127.0.0.1:9001",
  access_key=os.getenv('MINIO_ROOT_USER'),
  secret_key=os.getenv('MINIO_ROOT_PASSWORD'),
  secure=False
)

def upload_to_minio(df: pd.DataFrame, bucket_name: str, object_name: str) -> None:
  """
  Upload a Dataframe to MinIO as a CSV from memory
  """
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
    print(f"Failed to upload {object_name}: {e}")


def get_month_links(season: str) -> Optional[List[Tuple[str, str]]]:
  """
  Fetches the month links from the Basketball Reference website for a given NBA season.

  Inputs:
    season (str): The NBA season in the format 'YYYY-YY' (e.g., '2019-20').

  Returns:
    tuple: A tuple containing:
      - month_link_list (list of tuples): A list of tuples where each tuple contains:
        - name of the month (str): The name of the month (e.g., 'october', 'november').
        - url (str): The full URL to the page for that month.
  """
  try:
    start_year, end_year = season.split('-')
    start_year = int(start_year)
    if len(end_year) != 2 or not end_year.isdigit():
      raise ValueError
  except(ValueError, AttributeError):
    print(f"Invalid season format: {season}. Expected format is 'YYYY-YY'.")
    return None, None

  end_year_full = start_year + 1 if end_year == '00' else int(str(start_year)[:2] + end_year)
  
  start_url = f"{BASE_URL}/leagues/NBA_{end_year_full}_games.html"

  month_link_list = []
  try:
    response = requests.get(start_url)
    response.raise_for_status()
  except requests.exceptions.HTTPError:
    handle_http_error(response)
  except Exception as e:
    handle_general_error(e, start_url)
  
  soup = BeautifulSoup(response.text, 'html.parser')
  body = soup.find('body')

  div_elements = body.find_all('div', class_='filter')
  for div in div_elements:
    a_tags = div.find_all('a', href=True)
    for a_tag in a_tags:
      link_text = a_tag.text.strip().lower()
      if any(month in link_text for month in a_tag.text.strip().lower().split()):
        month_link_list.append((link_text, f"{BASE_URL}{a_tag['href']}"))
    
  return month_link_list




def get_box_score_links(month_link_list: List[Tuple[str, str]], 
                        start_date: Optional[str] = None, 
                        end_date: Optional[str] = None
                        ) -> Tuple[Optional[List[List[str]]], Optional[List[List[str]]]]:
  """ 
  Fetches box score links and corresponding game dates within a given date range (for batch scraping).

  Inputs:
    month_link_list (list of tuples): List of tuples where each tuple contains:
      - month (str): The name of the month (e.g., 'october').
      - page (str): The full URL to the page for that month.
    start_data: The start date for filtering games (format: YYYYMMDD)
    end_date: The end date for filtering games (format: YYYYMMDD)

  Returns:
    tuple: A tuple containing:
      - box_link_array (list of lists): A list of lists where each inner list contains the URLs to box scores for the games played in the given date range.
      - all_dates (list of lists): A list of lists where each inner list contains the corresponding dates (formatted as 'YYYYMMDD') for the box scores in the same order as `box_link_array`.
  """
  start_date_dt = None
  end_date_dt = None

  if start_date:
    try:
      start_date_dt = datetime.strptime(start_date, '%Y%m%d')
    except ValueError:
      raise ValueError(f"Invalid start date format: {start_date}. Expected format: YYYYMMDD.")
    
  if end_date:
    try:
      end_date_dt = datetime.strptime(end_date, '%Y%m%d')
    except ValueError:
      raise ValueError(f"Invalid end date format: {end_date}. Expected format: YYYYMMDD.")
    
  if start_date_dt and end_date_dt and start_date_dt > end_date_dt:
    raise ValueError("Start date cannot be after end date.")
    
  box_link_array = []
  all_dates = []

  for _, page in month_link_list:
    page_link_list = []
    page_date_list = []
    try:
      response = requests.get(page)
      response.raise_for_status()
      soup = BeautifulSoup(response.text, 'html.parser')

      table = soup.find_all('tbody')
      box_scores = table[0].find_all('a', href=True)

      for i in box_scores:
        if i.text.strip() == 'Box Score':
          date_parts = i.text.strip().split(', ')
          year = date_parts[2]
          day = date_parts[1].split(' ')[1].zfill(2)
          month_code = MONTH_DICT[date_parts[1].split(' ')[0]]
          game_date = f'{year}{month_code}{day}'
          game_date_dt = datetime.strptime(game_date, '%Y%m%d')
          
          if (start_date_dt and end_date_dt) and not (start_date_dt <= game_date_dt <= end_date_dt):
            continue

          page_link_list.append(f"{BASE_URL}{i['href']}")
          page_date_list.append(game_date)
      
      if page_link_list:
        box_link_array.append(page_link_list)
        all_dates.append(page_date_list)
      time.sleep(10)

    except requests.exceptions.HTTPError:
      handle_http_error(response)
    except Exception as e:
      handle_general_error(e, page)

  return box_link_array, all_dates




# from https://medium.com/@HeeebsInc/using-machine-learning-to-predict-daily-fantasy-basketball-scores-part-i-811de3c54a98
def extract_player_data(box_links: List[List[str]], 
                        all_dates: List[List[str]]
                        ) -> pd.DataFrame:
  """
  Extract player statistics from each box score link and save the data to a DataFrame.

  Inputs:
    box_links (list of lists): A list containing lists of URLs to box score pages.
    all_dates (list of lists): A list containing lists of dates corresponding to the box scores.

  Returns:
    stat_df (pd.DataFrame): A DataFrame containing the extracted player statistics.
  """
  df_columns = [
                'Date', 'Name', 'Team', 'Opponent', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA',
                '3P%','FT', 'FTA', 'FT%', 'ORB','DRB', 'TRB', 'AST', 'STL', 'BLK', 
                'TOV', 'PF', 'PTS', 'GmSc', '+-', 'GameLink', 'Home'
               ]
  
  stat_df = pd.DataFrame(columns=df_columns)

  for i, (links, dates) in enumerate(zip(box_links, all_dates)):
    print(f'Processing batch {i+1}/{len(box_links)}')

    for link, date in zip(links, dates):
      print(f'Scraping box score: {link} for game date {date}')
      
      try:
        response = requests.get(link)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, 'html.parser')

        home_team_abbr = link.split('/')[-1].split('.')[0][-3:]  # e.g. https://www.basketball-reference.com/boxscores/202310240DEN.html
        home_team = TEAM_ABBREVIATIONS.get(home_team_abbr, None)

        tables = soup.find_all('table', id=lambda x: x and x.endswith('-game-basic'))
        team_names = [table.find('caption').text.split(' Basic and Advanced Stats Table')[0].strip() for table in tables]
        for table in tables:
          team_name = table.find('caption').text.split(' Basic and Advanced Stats Table')[0].strip()
          opponent_name = team_names[1] if team_names[0] == team_name else team_names[0]
          rows = table.find('tbody').find_all('tr')

          is_home = 1 if team_name == home_team else 0

          for row in rows:
            if row.find('th').text in ['Team Totals', 'Reserves']:
              continue
            player_name = normalize_name(row.find('th').text.strip())

            stats = [date, player_name, team_name, opponent_name]

            dnp = row.find('td', {'data-stat': 'reason'})
            if dnp and 'Did Not Play' in dnp.text:
              stats += ['DNP'] * (len(df_columns) - 6)
            else:
              for td in row.find_all('td'):
                stats.append(td.text.strip() or '0')
            
            stats.append(link)
            stats.append(is_home)

            if len(stats) == len(df_columns):
              new_row = pd.DataFrame([stats], columns=df_columns)
              stat_df = pd.concat([stat_df, new_row], ignore_index=True)
            else:
              print(f'Skipping incomplete data for {player_name}')

      except requests.exceptions.HTTPError:
        handle_http_error(response)
      except Exception as e:
        handle_general_error(e, link)
      
      time.sleep(random.uniform(3, 7))

  return stat_df



if __name__ == "__main__":
  season = '2021-22'
  month_links = get_month_links(season)
  if month_links:
    print(f"Testing the first month links for the {season} season.")
    print(month_links)
    # test_month_links = month_links[:1]
    # box_score_links, all_dates = get_box_score_links(test_month_links)

    # if box_score_links and all_dates:
    #   print(f"Found {len(box_score_links)} sets of box score links.")
    #   limited_box_score_links = box_score_links[0][:5]
    #   limited_dates = all_dates[0][:5]
    #   stat_df = extract_player_data([limited_box_score_links], [limited_dates])
    #   save_dataframes_to_csv(stat_df, season, output_dir="data")
    # else:
    #   print("No box score links found.")
  else:
    print("No month links found.")
