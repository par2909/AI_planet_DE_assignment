import os

# Disable unsupported plugins
os.environ['METAFLOW_DEFAULT_DATASTORE'] = 'local'
os.environ['METAFLOW_STEP_DECORATORS'] = ''

from metaflow import FlowSpec, step
from sqlalchemy import create_engine
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

class ETLFlow(FlowSpec):

    @step
    def start(self):
        self.db_url = 'postgresql://postgres:parish@localhost:5432/airbnb_nyc'
        self.next(self.extract_data)

    @step
    def extract_data(self):
        engine = create_engine(self.db_url)
        self.df = pd.read_sql('SELECT * FROM airbnb_listings', engine)
        self.next(self.handle_missing_values)

    @step
    def handle_missing_values(self):
        self.df.fillna({'reviews_per_month': 0}, inplace=True)
        self.df.dropna(subset=['last_review'], inplace=True)
        self.next(self.normalize_data)

    @step
    def normalize_data(self):
        scaler = MinMaxScaler()
        numerical_cols = ['price', 'minimum_nights', 'number_of_reviews', 'reviews_per_month', 'calculated_host_listings_count', 'availability_365']
        self.df[numerical_cols] = scaler.fit_transform(self.df[numerical_cols])
        self.next(self.calculate_additional_metrics)

    @step
    def calculate_additional_metrics(self):
        self.df['avg_price_per_night'] = self.df['price'] / self.df['minimum_nights']
        self.df['review_density'] = self.df['reviews_per_month'] / self.df['minimum_nights']
        self.next(self.separate_date_time)

    @step
    def separate_date_time(self):
        self.df['last_review_date'] = pd.to_datetime(self.df['last_review']).dt.date
        self.df['last_review_time'] = pd.to_datetime(self.df['last_review']).dt.time
        self.next(self.calculate_avg_price_per_neighborhood)

    @step
    def calculate_avg_price_per_neighborhood(self):
        self.avg_price_neighborhood = self.df.groupby('neighbourhood')['price'].mean().reset_index()
        self.avg_price_neighborhood.columns = ['neighbourhood', 'avg_price']
        self.next(self.merge_metrics)

    @step
    def merge_metrics(self):
        self.df = self.df.merge(self.avg_price_neighborhood, on='neighbourhood', how='left')
        self.next(self.write_to_postgres)

    @step
    def write_to_postgres(self):
        engine = create_engine(self.db_url)
        self.df.to_sql('transformed_airbnb_listings', engine, if_exists='replace', index=False)
        print("Data successfully written to PostgreSQL table: transformed_airbnb_listings")
        self.next(self.end)

    @step
    def end(self):
        print("ETL workflow completed successfully.")

if __name__ == '__main__':
    ETLFlow()
