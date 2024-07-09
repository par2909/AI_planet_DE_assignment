import os

# Disable unsupported plugins
os.environ['METAFLOW_DEFAULT_DATASTORE'] = 'local'
os.environ['METAFLOW_STEP_DECORATORS'] = ''

from metaflow import FlowSpec, step
import pandas as pd
import opendatasets as od
from sqlalchemy import create_engine

class LoadDataFlow(FlowSpec):

    @step
    def start(self):
        self.dataset_url = 'https://www.kaggle.com/datasets/dgomonov/new-york-city-airbnb-open-data'
        self.next(self.download_dataset)

    @step
    def download_dataset(self):
        od.download(self.dataset_url)
        self.next(self.read_csv_file)

    @step
    def read_csv_file(self):
        self.csv_file_path = 'new-york-city-airbnb-open-data/AB_NYC_2019.csv'
        self.df = pd.read_csv(self.csv_file_path)
        self.next(self.write_to_postgres)

    @step
    def write_to_postgres(self):
        db_url = 'postgresql://postgres:parish@localhost:5432/airbnb_nyc'
        table_name = 'airbnb_listings'
        engine = create_engine(db_url)
        self.df.to_sql(table_name, engine, if_exists='replace', index=False)
        print("Data successfully written to PostgreSQL database.")
        self.next(self.end)

    @step
    def end(self):
        print("Load workflow completed successfully.")

if __name__ == '__main__':
    LoadDataFlow()
