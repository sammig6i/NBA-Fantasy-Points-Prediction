import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import unicodedata
import random

BASE_URL = 'https://www.basketball-reference.com'
month_dictionary = {'Jan': '01', 'Feb': '02',  'Mar': '03', 
'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Oct': '10', 'Nov': '11', 'Dec': '12'}


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
          
      If an HTTP error occurs, the function returns (None, None).
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
  except requests.exceptions.HTTPError as err:                  # TODO Add helper function for handling errors
    print(f"HTTP error occurred: {err}")
    return None, None
  
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
  """
  box_link_array = []
  all_dates = []

  for month, page in month_link_list:
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
          month_code = month_dictionary[date_parts[1].split(' ')[0]]
          formatted_date = f'{year}{month_code}{day}'
          page_date_list.append(formatted_date)
      box_link_array.append(page_link_list)
      all_dates.append(page_date_list)
      time.sleep(10)
    except requests.exceptions.HTTPError as err:              # TODO Add helper function for handling errors
      print(f"HTTP error occurred: {err}")
      return None, None
  return box_link_array, all_dates




# TODO UPDATE
# from https://medium.com/@HeeebsInc/using-machine-learning-to-predict-daily-fantasy-basketball-scores-part-i-811de3c54a98
def extract_player_data(box_links, all_dates, season):
  """
  
  """
  df_columns = ['Date', 'Name', 'Team', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA',
               '3P%','FT', 'FTA', 'FT%', 'ORB','DRB', 'TRB', 'AST', 'STL', 'BLK', 
               'TOV', 'PF', 'PTS', 'GmSc', '+-' ]
  stat_df = pd.DataFrame(columns = df_columns)
  error_df = pd.DataFrame(columns = ['URL', 'Error'])
  for i, (l, d) in enumerate(zip(box_links, all_dates)):
    print(f'Processing batch {i+1}/{len(box_links)}')
    for link, date in zip(l, d):
      print(f'{link}\n{date}')
      print(f'Currently Scraping {link}')
      
      try:
        response = requests.get(link)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, 'html.parser')

        tables = soup.find_all('table', id=lambda x: x and x.endswith('-game-basic'))
        for table in tables:
          caption = table.find('caption')
          team_name = caption.text.split(' Basic and Advanced Stats Table')[0].strip()
          rows = table.find('tbody').find_all('tr')
          for row in rows:
            if row.find('th').text in ['Team Totals', 'Reserves']:
              continue
            player_name = normalize_name(row.find('th').text.strip())
            dnp = row.find('td', {'data-stat': 'reason'})

            stats = [date, player_name, team_name]

            if dnp and 'Did Not Play' in dnp.text:
              stats += ['DNP'] * (len(df_columns) - 3)
            else:
              for td in row.find_all('td'):
                stat = td.text.strip()
                stats.append(stat if stat else 0)
  
            if len(stats) == len(df_columns):
              new_row = pd.DataFrame([stats], columns=df_columns)
              stat_df = pd.concat([stat_df, new_row], ignore_index=True)
            else:
              print(f'Skipping incomplete data for {player_name}')

        print(f'Finished Scraping: {link}')

      except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:                     # TODO Add helper function for handling errors
          print(f'Rate limit exceeded for {link}, Status code: {response.status_code}')
          print(f"Retry-After: {response.headers.get('Retry-After')}")
          time.sleep(int(response.get('Retry-After'), 60))
        else:
          print(f'HTTP error occurred: {http_err}')
        error = {'URL': link, 'Error': f'HTTPError: {http_err}'}
        new_row = pd.DataFrame([error])
        error_df = pd.concat([error_df, new_row], ignore_index=True)

      except Exception as e:                                  # TODO Add helper function for handling errors
        print(f"Error: {e}")
        print(f"Exception Type: {type(e).__name__}")
        error_message = str(e) if str(e) else "No error message provided."
        print(f'Error Scraping: {link}')
        error = {'URL': link, 'Error': error_message}
        new_row = pd.DataFrame([error])
        error_df = pd.concat([error_df, new_row], ignore_index=True)
      
      delay = random.uniform(3, 7)
      print(f"Delaying for {delay:.2f} seconds")
      time.sleep(delay)

  stat_df.to_csv(f'Season({season}).csv', lineterminator='\n', index=False)
  error_df.to_csv(f'Errors_Season({season}).csv', lineterminator='\n', index=False)
  
  message = f'Saved game stats for the {season} season to a csv'
  print(message)

      
def normalize_name(name):
  normalized = unicodedata.normalize('NFKD', name)
  without_diacritics = ''.join(c for c in normalized if not unicodedata.combining(c))
  return without_diacritics.lower()



if __name__ == "__main__":
  season = '2023-24'
  month_links = get_month_links(season)
  if month_links:
    print(f"Testing the first 2 month links for the {season} season.")
    test_month_links = month_links[:1]
    box_score_links, all_dates = get_box_score_links(test_month_links)

    print(f"Found {len(box_score_links)} sets of box score links.")
    print(f"{box_score_links}\n {all_dates}")
  else:
    print("No month links found.")
  