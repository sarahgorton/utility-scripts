#!/usr/bin/env python
# coding: utf-8

# # Run GA4 Reports

# #### Purpose
# Code to connect with GA4 (Google Analytics 4) and pull reports as needed.
# * API Docs https://developers.google.com/analytics/devguides/reporting/data/v1

# In[ ]:


# Resources

# Functionality for authenticating using Google Cloud Service Account credentials
from google.oauth2 import service_account

# Facilitates fetching and analyzing data from GA4 properties by building and executing reporting requests 
from google.analytics.data_v1beta import BetaAnalyticsDataClient

# The following fields are not available by default and must be explicitly imported
from google.analytics.data_v1beta.types import (
    DateRange,                # Specifies the start and end dates for the report
    Dimension,                # Defines the attributes (e.g., 'city', 'browser') by which to segment the data
    Metric,                   # Defines the quantitative data (e.g., 'sessions', 'users') to retrieve
    RunReportRequest,         # Core object for building a report query to send to GA4
    FilterExpression,         # Defines how filters are combined (e.g., AND/OR conditions)
    Filter,                   # A single condition used to filter a dimension or metric
    FilterExpressionList,     # A list of multiple FilterExpressions used in advanced filtering
    # NumericFilter,            # (Optional) Enables filtering metrics based on numeric conditions (e.g., > 100)
    # StringFilter,             # (Optional) Enables filtering dimensions based on string values (e.g., equals "Chrome")
)

# Provides powerful data structures like DataFrames and Series
import pandas as pd


# In[ ]:


# Establish connection with GA4

# 1. credentials which allow for authenticating the ga4 client
credentials_gcloud = service_account.Credentials.from_service_account_file('credentials_ga4.json') # Service Account Key from GCP

# 2. initialize the GA4 client 
client = BetaAnalyticsDataClient(credentials=credentials_gcloud)


# In[ ]:


# Function: run_report

def run_report(_property_id, _start_date, _end_date):
    """
    Runs a report within a specified Google Analytics 4 (GA4) property and outputs the results
    as a pandas DataFrame.
    https://developers.google.com/analytics/devguides/reporting/data/v1/basics

    Args:
        _property_id (str): The GA4 property ID used to identify which property's
        data to query. Ex. "111111111"
        _start_date (str): The report's desired start date. Ex. "2024-08-22"
        _end_date (str): The report's desired end date. Ex. "2024-08-28"

    Behavior:
        Return a dataframe which provides the count of sessions starts, segmented by their source / medium
    """
    
    # Define the date range to be applied based user entered start and end date
    _date_ranges = [DateRange(start_date = _start_date, end_date = _end_date)]
    

    # Define a dimension filter to be applied (Ex. on eventName, filter for session_start)
    _dimension_filter = FilterExpression(
        filter = Filter(
            field_name = "eventName", # Replacable
            string_filter = Filter.StringFilter(
                value = "session_start",  # Replacable
                match_type = "EXACT"  # Replacable
            )
        )
    )

    # Define the dimensions to be pulled
    _dimensions = [Dimension(name = "eventName"), Dimension(name = "firstUserSourceMedium")]

    # Define the metrics to be pulled
    _metrics = [Metric(name = "eventCount")] 

    # Build the report request with the conditions declared above
    request = RunReportRequest(
        property = f"properties/{_property_id}",
        date_ranges = _date_ranges,
        dimension_filter = _dimension_filter,
        dimensions = _dimensions,
        metrics = _metrics
    )
    
    # Run the report request with the conditions declared above
    response = client.run_report(request)

    # Structure the returned report
    response_structured = []
    for row in response.rows:
        first_dimension_value = row.dimension_values[0].value
        second_dimension_value = row.dimension_values[1].value
        metric_value = row.metric_values[0].value
        
        response_structured.append({
            'Event Name': first_dimension_value,
            'Source/Medium': second_dimension_value,
            'Event Count': metric_value
        })
        
    # Store response_structured in response_df to be returned to caller 
    response_df = pd.DataFrame(response_structured).sort_values(by=['Event Name', 'Source/Medium'], ascending=True)

    # Return the df
    return response_df



# In[ ]:


# TEST: run_report

# 1. Determine desired parameter values
property_id = "xx"
start_date = "xx"
end_date = "xx"

# 2. Call run_report and store returned df
report = run_report(property_id, start_date, end_date)
    
# 3. Preview df
report.head(5)


# In[ ]:




