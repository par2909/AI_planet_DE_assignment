from sqlalchemy import create_engine
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# Function to connect to PostgreSQL and execute SQL query
def execute_sql_query(query, db_url):
    engine = create_engine(db_url)
    df = pd.read_sql(query, engine)
    return df

# Function to separate date and time from a datetime column
def separate_date_time(df, datetime_column):
    df['{}_date'.format(datetime_column)] = pd.to_datetime(df[datetime_column]).dt.date
    df['{}_time'.format(datetime_column)] = pd.to_datetime(df[datetime_column]).dt.time
    return df

# Function to calculate average price per neighborhood
def calculate_avg_price_per_neighborhood(df, group_by_column, value_column):
    avg_price_neighborhood = df.groupby(group_by_column)[value_column].mean().reset_index()
    avg_price_neighborhood.columns = [group_by_column, 'avg_price']
    return avg_price_neighborhood

# Function to handle missing values
def handle_missing_values(df, fillna_dict=None, dropna_columns=None):
    if fillna_dict:
        df.fillna(fillna_dict, inplace=True)
    if dropna_columns:
        df.dropna(subset=dropna_columns, inplace=True)
    return df

# Function to merge calculated metrics back to the main dataframe
def merge_metrics(df_main, df_metrics, on_column, how='left'):
    df_main = df_main.merge(df_metrics, on=on_column, how=how)
    return df_main

# Function to normalize numerical columns
def normalize_data(df, numerical_cols):
    scaler = MinMaxScaler()
    df[numerical_cols] = scaler.fit_transform(df[numerical_cols])
    return df

# Function to calculate additional metrics
def calculate_additional_metrics(df):
    df['avg_price_per_night'] = df['price'] / df['minimum_nights']
    df['review_density'] = df['reviews_per_month'] / df['minimum_nights']
    return df

# Function to write DataFrame to PostgreSQL
def write_to_postgres(df, table_name, db_url):
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data successfully written to PostgreSQL table: {table_name}")

def main():
    # Database connection details
    db_url = 'postgresql://postgres:parish@localhost:5432/airbnb_nyc'

    # Execute SQL query to fetch data
    df = execute_sql_query('SELECT * FROM airbnb_listings', db_url)

    # Handle missing values before further processing
    df = handle_missing_values(df, fillna_dict={'reviews_per_month': 0}, dropna_columns=['last_review'])

    # Normalize numerical columns
    numerical_cols = ['price', 'minimum_nights', 'number_of_reviews', 'reviews_per_month', 'calculated_host_listings_count', 'availability_365']
    df = normalize_data(df, numerical_cols)

    # Calculate additional metrics
    df = calculate_additional_metrics(df)

    # Separate date and time from last_review column
    df = separate_date_time(df, 'last_review')

    # Calculate average price per neighborhood
    avg_price_neighborhood = calculate_avg_price_per_neighborhood(df, 'neighbourhood', 'price')

    # Merge calculated metrics back to the main dataframe
    df = merge_metrics(df, avg_price_neighborhood, 'neighbourhood')

    # Write transformed DataFrame to PostgreSQL
    write_to_postgres(df, 'transformed_airbnb_listings', db_url)

if __name__ == "__main__":
    main()
