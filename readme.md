# **ETL Workflow with Metaflow**

This repository contains Python scripts to perform an ETL (Extract, Transform, Load) process using Metaflow. The workflow includes two main steps: loading data from Kaggle and transforming it before storing it in a PostgreSQL database.

## **Prerequisites**
Before running the scripts, make sure you have the following installed on your machine:
    Python 3.6 or higher
    PostgreSQL database
    Kaggle API credentials (for downloading datasets)
    Metaflow (Python package)
    Required Python libraries: pandas, opendatasets, sqlalchemy, sklearn

## **Setup Instructions**
    Install PostgreSQL
    Ensure PostgreSQL is installed and running on your machine. Create a database named airbnb_nyc.

## **Install Python Libraries**
    Install the required Python libraries using pip:

    pip install pandas opendatasets sqlalchemy sklearn metaflow

## **Kaggle API Setup**
    To download datasets from Kaggle, you'll need to set up the Kaggle API:
        1. Sign in to your Kaggle account.
        2. Go to the "Account" section in your user profile.
        3. Scroll down to the "API" section and click "Create New API Token". This will download a kaggle.json file.
        4. Place the kaggle.json file in the ~/.kaggle/ directory (create the directory if it doesn't exist).


## **PostgreSQL Database Setup**
Create a PostgreSQL database named airbnb_nyc. You can use any database management tool to create
    Ensure your PostgreSQL database is running and accessible. Update the db_url in the scripts with your PostgreSQL username and password:

    db_url = 'postgresql://<username>:<password>@localhost:5432/airbnb_nyc'

    Replace <username> and <password> with your PostgreSQL credentials.

## **Workflow Explanation**
The workflow consists of two main steps: loading data from Kaggle and transforming it before storing it in
    
### load_flow.py 

        This script performs the following steps:
            Start: Initialize the dataset URL.
            Download Dataset: Download the dataset from Kaggle using opendatasets.
            Read CSV File: Read the downloaded CSV file into a Pandas DataFrame.
            Write to PostgreSQL: Write the DataFrame to a PostgreSQL database.


### etl_flow.py
        This script performs the following steps:

        Start: Initialize database connection details.
        Fetch Data: Fetch data from the PostgreSQL database.
        Handle Missing Values: Handle missing values in the data.
        Normalize Data: Normalize numerical columns.
        Calculate Additional Metrics: Calculate additional metrics.
        Separate Date and Time: Separate date and time from the last_review column.
        Calculate Average Price per Neighborhood: Calculate the average price per neighborhood.
        Write to PostgreSQL: Write the transformed DataFrame to the PostgreSQL database.

## **Running the Workflows**

    To run these workflows, use the following commands in windows terminal :

        set METAFLOW_DEFAULT_DATASTORE=local
        set METAFLOW_STEP_DECORATORS=
        python load_flow.py run
        python etl_flow.py run


    This will execute the ETL workflow, handling each step in sequence, ensuring the process is reproducible and fault-tolerant.


    This repository contains Python scripts for data ingestion (load.py) and ETL processes (etl.py) using Metaflow, along with instructions and troubleshooting tips.

# Error Explanation
## Error Encountered:
When running the scripts, you might encounter an error related to Metaflow attempting to load unsupported plugins like kubernetes or batch. This error typically manifests as a ModuleNotFoundError for modules such as fcntl.

### Example Error:

Traceback (most recent call last):
  File "load_flow.py", line 7, in <module>
    from metaflow import FlowSpec, step
  File "metaflow/__init__.py", line 115, in <module>
    from .plugins.datatools import S3
  File "metaflow/plugins/__init__.py", line 150, in <module>
    STEP_DECORATORS = resolve_plugins("step_decorator")
ValueError: Cannot locate step_decorator plugin 'kubernetes' at 'metaflow.plugins.kubernetes.kubernetes_decorator'

**LIMITATION AND WORKAROUND:**

Metaflow attempts to load plugins based on environment variables and configuration files. If unsupported plugins are being loaded despite efforts to disable them, follow these steps:

### Disable Plugins Explicitly:

Ensure that you set environment variables before importing Metaflow in your scripts:

	import os
	#Disable Metaflow plugins
	os.environ['METAFLOW_DEFAULT_DATASTORE'] = 'local'
	os.environ['METAFLOW_STEP_DECORATORS'] = ''
	#Check Metaflow Configuration:

Verify that Metaflow's configuration files (metaflow_config.yaml, etc.) do not override the environment variables set in your scripts.

**Update Dependencies:**

Ensure Metaflow and its dependencies are up to date. Sometimes, updating to the latest version can resolve plugin loading issues.

# Usage Instructions
### load.py
This script handles data ingestion from a Kaggle dataset into a PostgreSQL database.

Steps:
	Download the dataset using opendatasets.
	Read the CSV file into a Pandas DataFrame.
	Write the DataFrame to a PostgreSQL table.
	Example Usage:
			python load.py

### etl.py
This script performs ETL operations on data already stored in the PostgreSQL database.

Steps:
	Extract data from PostgreSQL.
	Handle missing values and normalize numerical columns.
	Calculate additional metrics and transform the data.
	Write the transformed data back to PostgreSQL.
	Example Usage:
		python etl.py

This README.md should help users understand how to use the scripts (load.py and etl.py) effectively while addressing common errors encountered with Metaflow plugin loading. Adjust the instructions as per your specific environment and requirements.

## **Conclusion**
  This repository demonstrates how to manage an ETL workflow using Metaflow. By breaking down the process into discrete steps, Metaflow ensures that the workflow is robust, reproducible, and easy to manage. 

	The following points summarize the main features and advantages of using Metaflow for this ETL process:
	1. Modular Workflow: Each stage of the ETL process is defined as a separate step in Metaflow, making the workflow modular and easy to understand.
	2. Reproducibility: Metaflow ensures that the workflow can be reproduced easily, allowing you to rerun the workflow from any step if needed.
	3.Error Handling: Metaflow handles errors gracefully, allowing you to retry steps or continue from the point of failure.
	4.Scalability: Metaflow can scale with your data, providing support for running workflows on larger datasets.