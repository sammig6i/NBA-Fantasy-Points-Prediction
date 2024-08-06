from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import pandas as pd

player_22_23_df = pd.read_csv('data/processed/Season(2022-23).csv')
player_23_24_df = pd.read_csv('data/processed/Season(2023-24).csv')


dates_2022_23 = player_22_23_df['Date'].unique()
dates_2023_24 = player_23_24_df['Date'].unique()
def reformat_date(date):
  date_str = str(date)
  return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

formatted_dates_2022_23 = [reformat_date(date) for date in dates_2022_23]
formatted_dates_2023_24 = [reformat_date(date) for date in dates_2023_24]

DOWNLOAD_PATH_2022_23 = os.path.abspath('data/raw/2022-23/')
DOWNLOAD_PATH_2023_24 = os.path.abspath('data/raw/2023-24/')

os.makedirs(DOWNLOAD_PATH_2022_23, exist_ok=True)
os.makedirs(DOWNLOAD_PATH_2023_24, exist_ok=True)

CHROMEDRIVER_PATH = '/usr/local/bin/chromedriver'

USERNAME = 'swghazzawi@gmail.com'
PASSWORD = 'nscVuZKNuP96svA'

chrome_options = Options()
chrome_options.add_argument("--headless")  
chrome_options.add_argument("--no-sandbox")  
chrome_options.add_argument("--disable-dev-shm-usage")  

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=chrome_options)

login_url = 'https://fantasydata.com/user/login?url=%2F%3F'
driver.get(login_url)

try:
    username_field = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, "email"))  
    )
    password_field = driver.find_element(By.ID, "password")  
    login_button = driver.find_element(By.ID, "submit") 

    username_field.send_keys(USERNAME)
    password_field.send_keys(PASSWORD)
    login_button.click()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "log-out")) 
    )
    print("Logged in successfully")
except Exception as e:
    print(f"Login failed: {e}")
    driver.quit()
    exit()


for date in formatted_dates_2022_23 + formatted_dates_2023_24:
    if date in formatted_dates_2022_23:
        download_path = DOWNLOAD_PATH_2022_23
    else:
        download_path = DOWNLOAD_PATH_2023_24

    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": download_path
    })


    print(f"Processing date: {date}")
    url = f"https://fantasydata.com/nba/fantasy-basketball-leaders?scope=game&date={date}&scoring=fpts_fanduel&order_by=fpts_fanduel"
    driver.get(url)
    
    try:
      csv_button = WebDriverWait(driver, 10).until(
          EC.element_to_be_clickable((By.CLASS_NAME, "csv"))
      )
      csv_button.click()
      
      time.sleep(5)  
      files = [f for f in os.listdir(download_path) if f.endswith(".csv")]
      if not files:
        print(f"File not found for {date}")
        continue

      latest_file = max([os.path.join(download_path, f) for f in files], key=os.path.getctime)
      new_name = os.path.join(download_path, f"{date}.csv")

      os.rename(latest_file, new_name)
      print(f"Downloaded and saved: {new_name}")

    except Exception as e:
        print(f"Failed to download CSV for {date}: {e}")

driver.quit()