import logging

import pandas as pd

from data_pipeline_services.config.variables import TEAM_ABBREVIATIONS


def validate_cleaned_data(df: pd.DataFrame) -> bool:
  """
  Ensure essential columns are valid after cleaning
  """
  try:
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    valid_teams = set(TEAM_ABBREVIATIONS.keys())
    if not df["Team"].isin(valid_teams).all() or not df["Opponent"].isin(valid_teams).all():
      logging.error("Invalid team abbreviations detected.")
      return False

    if df["GameLink"].isnull().any() or (df["GameLink"] == "").any():
      logging.error("Missing or empty GameLink values.")
      return False

    numeric_columns = [
      "MP",
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
      if not pd.api.types.is_numeric_dtype(df[col]):
        non_numeric = df[~pd.to_numeric(df[col], errors="coerce").notnull()]
        if not non_numeric.empty:
          logging.error(f"'{col}' contains non-numeric values. Examples: {non_numeric[col].head()}")
          return False

      if df[col].isnull().any():
        logging.warning(f"'{col}' contains NaN values.")

    logging.info("Validation passed.")
    return True
  except Exception as e:
    logging.error(f"Validation error: {e}")
    return False
