import pandas as pd
import os
import opendatasets as od
from sqlalchemy import create_engine

# Function to download dataset from Kaggle using opendatasets
def download_dataset(url):
    od.download(url)

# Function to read CSV file into a pandas DataFrame
def read_csv_file(filepath):
    df = pd.read_csv(filepath)
    return df

# Function to connect to PostgreSQL and write DataFrame to a table
def write_to_postgres(df, table_name, db_url):
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Data successfully written to PostgreSQL database.")

def main():
    # Assign the Kaggle data set URL into variable
    dataset_url = 'https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data'
    
    # Download dataset
    download_dataset(dataset_url)
    
    # Specify CSV file path
    csv_file_path = 'new-york-city-airbnb-open-data/AB_NYC_2019.csv'
    
    # Read CSV file into DataFrame
    df = read_csv_file(csv_file_path)
    
    # Database connection details
    db_url = 'postgresql://postgres:parish@localhost:5432/airbnb_nyc'
    
    # Define table name
    table_name = 'airbnb_listings'
    
    # Write DataFrame to PostgreSQL
    write_to_postgres(df, table_name, db_url)

if __name__ == "__main__":
    main()