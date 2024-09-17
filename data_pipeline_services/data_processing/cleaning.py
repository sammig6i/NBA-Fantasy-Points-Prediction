import hashlib
import logging
import os

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection
from validate import validate_cleaned_data

from data_pipeline_services.config.variables import TEAM_ABBREVIATIONS

load_dotenv()
logging.basicConfig(level=logging.ERROR)


def connect_db() -> connection | None:
  try:
    connection = psycopg2.connect(
      user=os.getenv("DB_USER"),
      password=os.getenv("DB_PASSWORD"),
      host=os.getenv("DB_HOST"),
      port=os.getenv("DB_PORT"),
      database=os.getenv("DB_NAME"),
    )
    return connection
  except Exception as err:
    logging.error(f"Error while connecting to PostgreSQL: {err}")
    return None


# Data Cleaning and Preprocessing
def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
  """
  Convert columns that should be numeric, coerce errors to NaN.
  """
  df = df.copy()
  numeric_columns = [
    "FG",
    "FGA",
    "FG%",
    "3P",
    "3PA",
    "3P%",
    "FT",
    "FTA",
    "FT%",
    "ORB",
    "DRB",
    "TRB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "PTS",
    "+-",
    "GmSc",
  ]

  for col in numeric_columns:
    df.loc[:, col] = pd.to_numeric(df[col].replace(["", "-", "DNP"], np.nan), errors="coerce")

  percentage_columns = ["FG%", "3P%", "FT%"]
  df.loc[:, percentage_columns] = df[percentage_columns].astype(float)

  return df


def convert_team_names_to_abbreviations(df: pd.DataFrame) -> pd.DataFrame:
  """
  Convert full team names to their abbreviations in the DataFrame.
  """
  TEAM_ABBREVIATIONS_REVERSED = {v: k for k, v in TEAM_ABBREVIATIONS.items()}

  df["Team"] = df["Team"].map(TEAM_ABBREVIATIONS_REVERSED)
  df["Opponent"] = df["Opponent"].map(TEAM_ABBREVIATIONS_REVERSED)

  return df


def remove_dnp_and_zero_minutes(df: pd.DataFrame) -> pd.DataFrame:
  """
  Remove rows where players either have 'DNP', empty string, or '0:00' in the MP column.
  """
  df = df[~df["MP"].isin(["DNP", "", "0:00"])]
  df = df[df["MP"].notna()]
  return df


def convert_mp_to_minutes(df: pd.DataFrame) -> pd.DataFrame:
  """
  Convert original MM:SS format in 'MP' column to total minutes as a float.
  """

  def to_minutes(x):
    if pd.isna(x) or x == "" or x == "DNP":
      return np.nan
    if isinstance(x, (int, float)):
      return float(x)
    try:
      if ":" in str(x):
        (minutes, seconds) = map(int, str(x).split(":"))
        return float(minutes) + float(seconds) / 60
      else:
        return float(x)
    except ValueError:
      return np.nan

  df = df.copy()
  df["MP"] = df["MP"].apply(to_minutes)
  df = df[df["MP"].notna()]
  return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
  """
  Remove duplicates from the dataset based on key columns.
  """
  df_cleaned = df.drop_duplicates(subset=["Date", "Team", "Opponent", "Name"])
  return df_cleaned


# Assign Unique IDs
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
  players = df["Name"].drop_duplicates()
  player_id_map = {}

  cursor = connection.cursor()

  for player_name in players:
    cursor.execute(
      """
      INSERT INTO Players (player_name)
      VALUES (%s)
      ON CONFLICT (player_name) DO NOTHING
      RETURNING player_id;
      """,
      (player_name,),
    )
    result = cursor.fetchone()
    if result:
      player_id_map[player_name] = result[0]
    else:
      cursor.execute(
        """
        SELECT player_id FROM Players 
        WHERE player_name = %s;
        """,
        (player_name,),
      )
      player_id_map[player_name] = cursor.fetchone()[0]

  connection.commit()
  return player_id_map


def assign_game_ids(df: pd.DataFrame, connection: connection) -> dict:
  """
  Assign unique game IDs to each game and populate the Games table.
  """
  games = df[["Date", "Team", "Opponent", "Home", "GameLink"]].drop_duplicates()
  game_id_map = {}

  cursor = connection.cursor()
  for game in games.itertuples():
    game_date = game.Date

    if game.Home == 1:
      home_team = game.Team
      away_team = game.Opponent
    else:
      home_team = game.Opponent
      away_team = game.Team

    game_link = game.GameLink
    game_id = generate_game_id(game_date, home_team, away_team)

    cursor.execute(
      """
      INSERT INTO Games (game_id, game_date, home_team, away_team, game_link) 
      VALUES (%s, %s, %s, %s, %s) 
      ON CONFLICT (game_link) DO NOTHING 
      RETURNING game_id;
      """,
      (game_id, game_date, home_team, away_team, game_link),
    )

    game_id_map[(game_date, home_team, away_team)] = game_id

  connection.commit()
  return game_id_map


# Prepare Player Stats
def clean_and_prepare_player_stats(
  df: pd.DataFrame, player_id_map: dict, game_id_map: dict, connection: connection
) -> None:
  """
  Insert the cleaned player stats into the PlayerStats table in the database.
  """
  cursor = connection.cursor()

  for row in df.to_dict("records"):
    game_date = row["Date"]
    team = row["Team"]
    opponent = row["Opponent"]
    home_status = row["Home"]

    if home_status == 1:
      home_team = team
      away_team = opponent
    else:
      home_team = opponent
      away_team = team

    player_id = player_id_map.get(row["Name"])
    game_id = game_id_map.get((game_date, home_team, away_team))

    if player_id and game_id:
      cursor.execute(
        """
        INSERT INTO PlayerStats (
        game_id, player_id, team, opponent, mp, fg, fga, fg_percent, three_p, three_pa, 
        three_p_percent, ft, fta, ft_percent, orb, drb, trb, ast, stl, blk, 
        tov, pf, pts, gmsc, plus_minus
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
          game_id,
          player_id,
          row["Team"],
          row["Opponent"],
          row["MP"],
          row["FG"],
          row["FGA"],
          row["FG%"],
          row["3P"],
          row["3PA"],
          row["3P%"],
          row["FT"],
          row["FTA"],
          row["FT%"],
          row["ORB"],
          row["DRB"],
          row["TRB"],
          row["AST"],
          row["STL"],
          row["BLK"],
          row["TOV"],
          row["PF"],
          row["PTS"],
          row["GmSc"],
          row["+-"],
        ),
      )

  connection.commit()


# Process Raw Data
def process_raw_data(df: pd.DataFrame) -> None:
  connection = None
  try:
    logging.info("Starting data processing...")

    df = df.copy()

    # DB connection
    connection = connect_db()
    if not connection:
      print("Database connection failed.")
      return

    logging.info("Database connected.")

    # Data cleaning, preprocessing, and validate
    logging.info("Cleaning and preprocessing data...")

    df = remove_duplicates(df)
    df = convert_team_names_to_abbreviations(df)
    df = remove_dnp_and_zero_minutes(df)
    df = convert_mp_to_minutes(df)
    df = clean_numeric_columns(df)

    if not validate_cleaned_data(df):
      logging.error("Data validation failed.")
      return

    # assign unique IDs for players and games
    logging.info("Assigning unique IDs for players and games...")
    player_id_map = assign_player_ids(df, connection)
    game_id_map = assign_game_ids(df, connection)

    # Insert cleaned player stats into database
    logging.info("Inserting player stats into database...")
    clean_and_prepare_player_stats(df, player_id_map, game_id_map, connection)
    logging.info("Data processing completed successfully.")
  except Exception as e:
    logging.error(f"Error processing raw data: {e}")
  finally:
    if connection:
      connection.close()
      logging.info("Database connection closed.")
