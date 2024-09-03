import requests
from bs4 import BeautifulSoup

BASE_URL = 'https://www.basketball-reference.com'

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
  except requests.exceptions.HTTPError as err:
    if response.status_code == 429:
      print("Rate limit exceeded. Please try again later.") #TODO Add helper function for handling errors
    else:
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


def get_box_score_links(month_link_list): # TODO UPDATE
  base_url = 'https://www.basketball-reference.com'
  page_to_check_dict = {'Month': [], 'Url': [], 'Index': []}
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
          page_link_list.append(f"{base_url}{i['href']}")
        if ',' in i.text.strip():
            date = i.text.strip()
            date = date.split(', ')
            year = date[2]
            date = date[1].split(' ')
            day = f'0{date[1]}' if len(date[1]) == 1 else date[1]

            mon = month_dictionary[date[0]]
            date = f'{year}{mon}{day}'
            page_date_list.append(date)
      if len(page_link_list) == 0 or len(box_scores)/len(page_link_list) != 4:
          page_to_check_dict['Url'].append(page)
          page_to_check_dict['Month'].append(month)
          page_to_check_dict['Index'].append(len(page_link_list))
      else:
          page_to_check_dict['Url'].append(page)
          page_to_check_dict['Month'].append(month)
          page_to_check_dict['Index'].append(None)
      box_link_array.append(page_link_list)
      all_dates.append(page_date_list)
      time.sleep(10)
    except HTTPError as err:
      if response.status_code == 429:
        print("Rate limit exceeded. Please try again later.")
      else:
        print(f"HTTP error occurred: {err}")
      return None, None
  return box_link_array, all_dates


if __name__ == "__main__":
  month_links = get_month_links('2023-24')
  season = '2023-24'
  if month_links:
    print(f"Found {len(month_links)} month links for the {season} season:")
    for month, link in month_links:
      print(f"{month}: {link}")
  else:
    print("No month links found.")