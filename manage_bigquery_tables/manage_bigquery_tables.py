#!/usr/bin/env python
# coding: utf-8

# # Manage BigQuery Tables

# #### Purpose
# Code to connect with BigQuery and delete, create, and load tables as needed

# In[ ]:


# Resources

# functionality for authenticating using Google Cloud Service Account credentials
from google.oauth2 import service_account

# allows interaction with bigquery 
from google.cloud import bigquery

# work with dfs
import pandas as pd

# work with numbers
import numpy as np


# In[ ]:


# Establish connection with BigQuery

# 1. credentials which allow for authenticating the bigquery client
credentials_gcloud = service_account.Credentials.from_service_account_file('xx.json') # Service Account Key from GCP

# 2. add scopes which define what actions the bigquery client can take 
credentials_gcloud_scoped = credentials_gcloud.with_scopes(
    ['https://www.googleapis.com/auth/cloud-platform']) # google cloud platform - access to bigquery 

# 3. create the bigquery client which allows us to run jobs (running a query, pulling dataset info, etc.)
bigquery_client = bigquery.Client(credentials=credentials_gcloud_scoped, project=credentials_gcloud.project_id,)


# In[ ]:


# Determine projects available

# 1. Get an iterator for all projects and convert to a list
project_ids = sorted(
    (p.project_id for p in bigquery_client.list_projects())
)

# 2. Loop through the list of tables and print each table's ID
for project_id in project_ids:
    print(project_id)
    


# In[ ]:


# Determine datasets available

# 1. Determine desired project (assign project_ref with one of the project_ids printed above)
project_ref = "xx"

# 2. Get an iterator for all datasets in a given project and convert to a list
dataset_ids = sorted(
    (d.dataset_id for d in bigquery_client.list_datasets(project_ref))
)

# 3. Loop through the list of tables and print each table's ID
for dataset_id in dataset_ids:
    print(dataset_id)
    


# In[ ]:


# Determine tables available

# 1. Determine desired dataset (assign dataset_ref with one of the dataset_ids printed above)
dataset_ref = "xx"
project_dataset_ref = bigquery.DatasetReference(project_ref, dataset_ref)

# 2. Get an iterator for all tables in a given project.dataset and convert to a list
table_ids = sorted(
    (t.table_id for t in bigquery_client.list_tables(project_dataset_ref))
)

# 3. Loop through the list of tables and print each table's ID
for table_id in table_ids:
    print(table_id)
    


# In[ ]:


# Function: delete_table

def delete_table(_project_dataset_table_ref):
    """
    Deletes a BigQuery table given its TableReference.

    Args:
        _project_dataset_table_ref (google.cloud.bigquery.table.TableReference):
            A fully qualified reference to the BigQuery table to delete.

    Behavior:
        Deletes the specified table if it exists.
        If the table does not exist, no error is raised (not_found_ok=True).
    """
    # Deletes a BigQuery table given its TableReference
    bigquery_client.delete_table(_project_dataset_table_ref, not_found_ok=True)


# In[ ]:


# # TEST: delete_table

# # 1. Determine desired table (assign table_ref with one of the table_ids printed above)
# table_ref = "xx"
# project_dataset_table_ref = bigquery.TableReference(project_dataset_ref, table_ref)

# # 2. Call delete_table
# delete_table(project_dataset_table_ref)


# In[ ]:


# Function: create_and_load_table

def create_and_load_table(_project_dataset_table_ref, _df_to_load):
    """
    Create a BigQuery table given its TableReference.
    Load a dataframe into the created table.

    Args:
        _project_dataset_table_ref (google.cloud.bigquery.table.TableReference):
            A fully qualified reference to the BigQuery table to create.
        _df_to_load
            A df (dataframe) object to be loaded to BigQuery.

    Behavior:
        Creates the specified table and loads it with the specified df.
    """
    # Create a BigQuery table given its TableReference
    bigquery_client.create_table(_project_dataset_table_ref)

    # Load a dataframe into the created table
    job_config_desired = bigquery.LoadJobConfig()
    job_config_desired.autodetect = True
        # Ensure schema will be auto detected 
    job = bigquery_client.load_table_from_dataframe(_df_to_load, _project_dataset_table_ref, job_config=job_config_desired)
    job.result()  
        # Run the load process


# In[ ]:


# # # TEST: create_and_load_table

# # 1. Determine desired table (assign table_ref with one of the table_ids printed above)
# table_ref = "xx"
# project_dataset_table_ref = bigquery.TableReference(project_dataset_ref, table_ref)


# # 2. Create a test DataFrame with 5 rows and 3 columns of random integers
# df_to_load = pd.DataFrame({
#     'A': np.random.randint(0, 100, size=5),
#     'B': np.random.randn(5),              # random floats from normal distribution
#     'C': np.random.choice(['X', 'Y', 'Z'], size=5)  # random categorical data
# })

# # 3. Call delete_table
# create_and_load_table(project_dataset_table_ref, df_to_load)


# In[ ]:




