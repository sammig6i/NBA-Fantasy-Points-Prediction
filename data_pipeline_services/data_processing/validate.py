import pandas as pd
import logging
from data_ingestion.config import TEAM_ABBREVIATIONS

def validate_cleaned_data(df: pd.DataFrame) -> bool:
  """
  Ensure essential columns are valid after cleaning
  """
  try:
    pd.to_datetime(df['Date'], format='%Y%m%d', errors='raise').dt.strftime('%Y-%m-%d')

    valid_teams = set(TEAM_ABBREVIATIONS.keys())
    if not df['Team'].isin(valid_teams).all() or not df['Opponent'].isin(valid_teams).all():
      logging.error("Invalid team abbreviations detected.")
      return False

    if df['GameLink'].isnull().any() or (df['GameLink'] == '').any():
      logging.error("Missing or empty GameLink values.")
      return False
    
    if df['MP'].min() < 0:
      logging.error("'MP' (minutes played) cannot be negative.")
      return False

    if not pd.api.types.is_numeric_dtype(df['GmSc']):
      logging.error("'GmSc' contains non-numeric values.")
      return False
    
    if not pd.api.types.is_numeric_dtype(df['+-']):
      logging.error("'+-' (Plus/Minus) contains non-numeric values.")
      return False

    numeric_columns = ['FG', 'FGA', '3P', '3PA', 'FT', 'FTA', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']
    for col in numeric_columns:
      if not pd.api.types.is_numeric_dtype(df[col]):
        logging.error(f"'{col}' contains non-numeric values.")
        return False
      if df[col].min() < 0:
        logging.error(f"'{col}' has negative values, which is invalid.")
        return False

    percentage_columns = ['FG%', '3P%', 'FT%']
    for col in percentage_columns:
      if not pd.api.types.is_numeric_dtype(df[col]):
        logging.error(f"'{col}' contains non-numeric values.")
        return False
      if df[col].min() < 0 or df[col].max() > 1.0:
        logging.error(f"'{col}' must be between 0 and 100.")
        return False

    logging.info("Validation passed.")
    return True
  except Exception as e:
      logging.error(f"Validation error: {e}")
      return False