import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import random
from utils import normalize_name, save_dataframes_to_csv, handle_http_error, handle_general_error
from config import BASE_URL, MONTH_DICT, TEAM_ABBREVIATIONS


def get_month_links(season):
  """
  Fetches the month links from the Basketball Reference website for a given NBA season.

  Inputs:
    season (str): The NBA season in the format 'YYYY-YY' (e.g., '2019-20').

  Returns:
    tuple: A tuple containing:
      - month_link_list (list of tuples): A list of tuples where each tuple contains:
        - link_text (str): The name of the month (e.g., 'october', 'november').
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




def get_box_score_links(month_link_list):
  """ 
  Fetches box score links and corresponding game dates from the given list of month page links.

  Inputs:
    month_link_list (list of tuples): List of tuples where each tuple contains:
      - month (str): The name of the month (e.g., 'october').
      - page (str): The full URL to the page for that month.

  Returns:
    tuple: A tuple containing:
      - box_link_array (list of lists): A list of lists where each inner list contains the URLs to box scores for the games played in the given month.
      - all_dates (list of lists): A list of lists where each inner list contains the corresponding dates (formatted as 'YYYYMMDD') for the box scores in the same order as `box_link_array`.

  If an HTTP error occurs, the function returns (None, None).
  """
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
          page_link_list.append(f"{BASE_URL}{i['href']}")
        if ',' in i.text.strip():
          date_parts = i.text.strip().split(', ')
          year = date_parts[2]
          day = date_parts[1].split(' ')[1].zfill(2)
          month_code = MONTH_DICT[date_parts[1].split(' ')[0]]
          formatted_date = f'{year}{month_code}{day}'
          page_date_list.append(formatted_date)
      box_link_array.append(page_link_list)
      all_dates.append(page_date_list)
      time.sleep(10)
    except requests.exceptions.HTTPError:
      handle_http_error(response)
    except Exception as e:
      handle_general_error(e, page)

  return box_link_array, all_dates




# from https://medium.com/@HeeebsInc/using-machine-learning-to-predict-daily-fantasy-basketball-scores-part-i-811de3c54a98
def extract_player_data(box_links, all_dates):
  """
  Extract player statistics from each box score link and save the data to a DataFrame.

  Inputs:
    box_links (list of lists): A list containing lists of URLs to box score pages.
    all_dates (list of lists): A list containing lists of dates corresponding to the box scores.
    season (str): The NBA season in the format 'YYYY-YY' (e.g., '2023-24').

  Returns:
    stat_df (pd.DataFrame): A DataFrame containing the extracted player statistics.
  """
  df_columns = [
                'Date', 'Name', 'Team', 'Opponent', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA',
                '3P%','FT', 'FTA', 'FT%', 'ORB','DRB', 'TRB', 'AST', 'STL', 'BLK', 
                'TOV', 'PF', 'PTS', 'GmSc', '+-', 'GameLink'
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

        tables = soup.find_all('table', id=lambda x: x and x.endswith('-game-basic'))
        team_names = [table.find('caption').text.split(' Basic and Advanced Stats Table')[0].strip() for table in tables]

        for table in tables:
          team_name = table.find('caption').text.split(' Basic and Advanced Stats Table')[0].strip()
          opponent_name = team_names[1] if team_names[0] == team_name else team_names[0]
          rows = table.find('tbody').find_all('tr')

          for row in rows:
            if row.find('th').text in ['Team Totals', 'Reserves']:
              continue
            player_name = normalize_name(row.find('th').text.strip())

            stats = [date, player_name, team_name, opponent_name]

            dnp = row.find('td', {'data-stat': 'reason'})
            if dnp and 'Did Not Play' in dnp.text:
              stats += ['DNP'] * (len(df_columns) - 5)
            else:
              for td in row.find_all('td'):
                stats.append(td.text.strip() or '0')
            
            stats.append(link)

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
    test_month_links = month_links[:1]
    box_score_links, all_dates = get_box_score_links(test_month_links)

    if box_score_links and all_dates:
      print(f"Found {len(box_score_links)} sets of box score links.")
      limited_box_score_links = box_score_links[0][:5]
      limited_dates = all_dates[0][:5]
      stat_df = extract_player_data([limited_box_score_links], [limited_dates])
      save_dataframes_to_csv(stat_df, season, output_dir="data")
    else:
      print("No box score links found.")
  else:
    print("No month links found.")
