import os
import pandas as pd
import psycopg2
import logging
from dotenv import load_dotenv
from psycopg2.extensions import connection
from data_ingestion.config import TEAM_ABBREVIATIONS
import hashlib

load_dotenv()
logging.basicConfig(level=logging.ERROR)

def connect_db() -> connection | None:
  try:
    connection = psycopg2.connect(
      user=os.getenv("DB_USER"),
      password=os.getenv("DB_PASSWORD"),
      host=os.getenv("DB_HOST"),
      port=os.getenv("DB_PORT"),
      database=os.getenv("DB_NAME")
    )
    return connection
  except Exception as err:
    logging.error(f"Error while connecting to PostgreSQL: {err}")
    return None
  

def load_raw_data(file_path: str) -> pd.DataFrame:
  """
  Load raw csv file into pandas Dataframe.
  """
  df = pd.read_csv(file_path)
  return df


def convert_team_names_to_abbreviations(df: pd.DataFrame) -> pd.DataFrame:
  """
  Convert full team names to their abbreviations in the DataFrame.
  """
  TEAM_ABBREVIATIONS_REVERSED = {v: k for k, v in TEAM_ABBREVIATIONS.items()}

  df['Team'] = df['Team'].map(TEAM_ABBREVIATIONS_REVERSED)
  df['Opponent'] = df['Opponent'].map(TEAM_ABBREVIATIONS_REVERSED)
  
  return df


def remove_dnp_and_zero_minutes(df: pd.DataFrame) -> pd.DataFrame:
  """
  Remove rows where players either have 'DNP' in the stats or 0 minutes played (MP == '0:00').
  """
  df = df[~df['MP'].isin(['DNP', '0:00'])]
  return df


def convert_mp_to_minutes(df: pd.DataFrame) -> pd.DataFrame:
  """
  Convert original MM:SS format in 'MP' column to total minutes as a float.
  """
  df['MP'] = df['MP'].apply(lambda x: sum(int(t) * 60**i for i, t in enumerate(reversed(x.split(':')))) / 60 if isinstance(x, str) else 0)
  return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
  """
  Remove duplicates from the dataset based on key columns.
  """
  df_cleaned = df.drop_duplicates(subset=['Date', 'Team', 'Opponent', 'Name'])
  return df_cleaned


def generate_game_id(game_date: str, team: str, opponent: str) -> str:
  """
  Generate a unique ID by hashing combination of game date, home team, and away team
  """
  game_key = f"{game_date}_{team}_{opponent}"
  game_id = hashlib.md5(game_key.encode()).hexdigest()
  return game_id
  

def assign_player_ids(df: pd.DataFrame, connection: connection) -> dict:
  """
  Assign unique player IDs to each player in the dataset and populate the Players table.
  """
  players = df['Name'].unique()
  player_id_map = {}

  cursor = connection.cursor()

  for player in players:
    cursor.execute("INSERT INTO Players (player_name) VALUES (%s) ON CONFLICT (player_name) DO NOTHING RETURNING player_id;", (player,))
    result = cursor.fetchone()
    if result:
      player_id_map[player] = result[0]
    else:
      cursor.execute("SELECT player_id FROM Players WHERE player_name = %s;", (player,))
      player_id_map[player] = cursor.fetchone()[0]

  connection.commit()
  return player_id_map


def assign_game_ids(df: pd.DataFrame, connection: connection) -> dict:
  """
  Assign unique game IDs to each game and populate the Games table.
  """
  games = df[['Date', 'Team', 'Opponent', 'Home','GameLink']].drop_duplicates()
  game_id_map = {}

  cursor = connection.cursor()
  for index, game in games.iterrows():
    game_date = pd.to_datetime(game['Date'], format='%Y%m%d').strftime('%Y-%m-%d')
    
    if game['Home'] == 1:
      home_team = game['Team']
      away_team = game['Opponent']
    else:
      home_team = game['Opponent']
      away_team = game['Team']
    
    game_link = game['GameLink']
    game_id = generate_game_id(game_date, home_team, away_team)

    cursor.execute("""
            INSERT INTO Games (game_id, game_date, home_team, away_team, game_link) 
            VALUES (%s, %s, %s, %s, %s) 
            ON CONFLICT (game_link) DO NOTHING 
            RETURNING game_id;
        """, (game_id, game_date, home_team, away_team, game_link))
    
    game_id_map[(game_date, home_team, away_team)] = game_id

  connection.commit()
  return game_id_map


def clean_and_prepare_player_stats(df: pd.DataFrame, player_id_map: dict, game_id_map: dict, connection: connection) -> None:
  """
  Insert the cleaned player stats into the PlayerStats table in the database.
  """
  cursor = connection.cursor()

  for index, row in df.iterrows():
    game_date = pd.to_datetime(row['Date'], format='%Y%m%d').strftime('%Y-%m-%d')
    team = row['Team']
    opponent = row['Opponent']
    home_status = row['Home']

    if home_status == 1:
      home_team = team
      away_team = opponent
    else:
      home_team = opponent
      away_team = team

    player_id = player_id_map.get(row['Name'])
    game_id = game_id_map.get((game_date, home_team, away_team))

    if player_id and game_id:
      cursor.execute("""
          INSERT INTO PlayerStats (
              game_id, player_id, team, opponent, mp, fg, fga, fg_percent, three_p, three_pa, 
              three_p_percent, ft, fta, ft_percent, orb, drb, trb, ast, stl, blk, 
              tov, pf, pts, gmsc, plus_minus
          ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
      """, (
          game_id, player_id, row['Team'], row['Opponent'], row['MP'], row['FG'], row['FGA'], row['FG%'], 
          row['3P'], row['3PA'], row['3P%'], row['FT'], row['FTA'], row['FT%'], row['ORB'], row['DRB'], 
          row['TRB'], row['AST'], row['STL'], row['BLK'], row['TOV'], row['PF'], row['PTS'], row['GmSc'], 
          row['+-']
      ))

  connection.commit()


def process_raw_data(file_path: str) -> None:
  try:
    # 1 DB connection
    connection = connect_db()
    if not connection:
      print("Database connection failed.")
      return

    #2 load raw data
    raw_df = load_raw_data(file_path)

    #3 Data cleaning and preprocessing
    df = remove_duplicates(raw_df)
    df = convert_team_names_to_abbreviations(df)
    df = remove_dnp_and_zero_minutes(df)
    df = convert_mp_to_minutes(df)

    #4 assign unique IDs for players and games
    player_id_map = assign_player_ids(df, connection)
    game_id_map = assign_game_ids(df, connection)

    #5 Insert cleaned player stats into database
    clean_and_prepare_player_stats(df, player_id_map, game_id_map, connection)
  except Exception as e:
    print(f"Error processing raw data: {e}")
  finally:
    connection.close()


#! Validate columns to be correct data types '+-' -> positive/negative int, 'GmSc' -> positive/negative float

if __name__ == "__main__":
  file_path = os.path.join(os.path.dirname(__file__), '../data_ingestion/data/2021-22_Season.csv')
  process_raw_data(file_path)
  
