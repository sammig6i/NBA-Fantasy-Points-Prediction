import os
import pandas as pd
import psycopg2
import logging
from dotenv import load_dotenv
from psycopg2.extensions import connection

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
  games = df[['Date', 'Team', 'Opponent', 'GameLink']].drop_duplicates()
  game_id_map = {}

  cursor = connection.cursor()
  for index, game in games.iterrows():
    game_date = pd.to_datetime(game['Date'], format='%Y%m%d').strftime('%Y-%m-%d')
    home_team = game['Team']
    away_team = game['Opponent']
    game_link = game['GameLink']

    cursor.execute("""
            INSERT INTO Games (game_date, home_team, away_team, game_link) 
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (game_link) DO NOTHING 
            RETURNING game_id;
        """, (game_date, home_team, away_team, game_link))
    result = cursor.fetchone()
    if result:
      game_id_map[(game_date, home_team, away_team)] = result[0]
    else:
      cursor.execute("SELECT game_id FROM Games WHERE game_link = %s;", (game_link,))
      game_id_map[(game_date, home_team, away_team)] = cursor.fetchone()[0]

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
    game_link = row['GameLink']

    player_id = player_id_map.get(row['Name'])
    game_id = game_id_map.get((game_date, team, opponent))

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


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
  """
  Remove duplicates from the dataset based on key columns.
  """
  df_cleaned = df.drop_duplicates(subset=['Date', 'Team', 'Opponent', 'Name'])
  return df_cleaned


def process_raw_data(file_path: str) -> None:
  connection = connect_db()
  if not connection:
    print("Database connection failed.")
    return

  raw_df = load_raw_data(file_path)

  cleaned_df = remove_duplicates(raw_df)

  player_id_map = assign_player_ids(cleaned_df, connection)
  game_id_map = assign_game_ids(cleaned_df, connection)

  clean_and_prepare_player_stats(cleaned_df, player_id_map, game_id_map, connection)

  connection.close()


if __name__ == "__main__":
  file_path = '../data_ingestion/data/2021-22_Season.csv'
  process_raw_data(file_path)