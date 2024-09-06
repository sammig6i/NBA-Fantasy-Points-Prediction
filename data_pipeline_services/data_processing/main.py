import pandas as pd


def load_raw_data(file_path):
  """
  Load the raw CSV file into a pandas DataFrame.
  """
  df = pd.read_csv(file_path)
  return df

if __name__ == "__main__":
  file_path = '../data_ingestion/data/2021-22_Season.csv'
  raw_df = load_raw_data(file_path)
