# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Authorization

# COMMAND ----------

!pip install msal

# COMMAND ----------

# Importing libraries
import pandas as pd
import requests
import re
import numpy as np
from msal import ConfidentialClientApplication

# COMMAND ----------

# Defining env variables
client_id = dbutils.widgets.get('client_id')
client_credential = dbutils.widgets.get('client_credential')
tennant_id = dbutils.widgets.get('tennant_id')

authority = f'https://login.microsoftonline.com/{tennant_id}'
scope = ['https://graph.microsoft.com/.default']

# Authorization
client = ConfidentialClientApplication(
    client_id=client_id,
    client_credential=client_credential,
    authority=authority
)

token_result = client.acquire_token_silent(scopes=scope, account=None)

if token_result and 'access_token' in token_result:
    access_token = 'Bearer ' + token_result['access_token']
    print('Access token was loaded from cache')
else:
    token_result = client.acquire_token_for_client(scopes=scope)
    if 'access_token' in token_result:
        access_token = 'Bearer ' + token_result['access_token']
        print('New access token was acquired from Azure AD')
    else:
        raise Exception("Failed to acquire access token")

print(access_token)

headers = {
    'Authorization': access_token,
    'Content-Type': 'application/json'
}

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Get all Planner plan details (categories)

# COMMAND ----------

plan_id = dbutils.widgets.get('plan_id')
url_plan_details = f'https://graph.microsoft.com/v1.0/planner/plans/{plan_id}/details'

# COMMAND ----------

res_plan = requests.get(url_plan_details, headers=headers)

# COMMAND ----------

# Extracting only the categories from the JSON response
categories_dict = res_plan.json()['categoryDescriptions']

# COMMAND ----------

# Create a dataframe from the dictionary
categories = pd.DataFrame.from_dict(categories_dict, orient='index').reset_index()
categories.rename(columns={'index': 'id', 0: 'description'}, inplace=True)
categories = categories[categories['description'].notnull()]

# COMMAND ----------

# Write the dataframe in a table at sandbox database in hive_metastore
planner_categories = spark.createDataFrame(categories).write.format('delta').mode('overwrite').saveAsTable('sandbox.planner_categories')


# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Get all Planner Tasks for a Plan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Extract all tasks

# COMMAND ----------

url_tasks = f'https://graph.microsoft.com/v1.0/planner/plans/{plan_id}/tasks'

# COMMAND ----------

res_tasks = requests.get(url_tasks, headers=headers)

# COMMAND ----------

# Create a dataframe from the JSON response (using pagination)
tasks = pd.DataFrame(res_tasks.json()['value'])
page = 1

while '@odata.nextLink' in res_tasks.json().keys():
    res_tasks = requests.get(res_tasks.json()['@odata.nextLink'], headers=headers)
    tasks2 = pd.DataFrame(res_tasks.json()['value'])
    tasks = pd.concat([tasks, tasks2], ignore_index=True)
    page += 1
else:
    print(f'Pagination completed: {page} pages were successfully written to a dataframe!')


# COMMAND ----------

# Selecting columns wanted
tasks = tasks[['planId', 'bucketId', 'id', 'title', 'percentComplete', 'startDateTime', 'createdDateTime', 'dueDateTime', 'completedDateTime', 'completedBy', 'priority', 'createdBy', 'appliedCategories', 'assignments']]

# COMMAND ----------

# Using regex to extract the information from the task title into different columns of a dataframe
def extract_task_title_items(df):
    title = df['title']
    pattern = r'(.*) ?\[(\d*)\] *[Xx] *([^\[]*) \[(\d*)\] ?\| ?\[([\dD]{2}\/[\dM]{2}\/[\dA]{4})\]'
    match = re.match(pattern, title)
    
    if match:
        industry_id = match.group(2) or np.nan
        industry = match.group(1) or np.nan
        id = match.group(4) or np.nan
        description = match.group(3) or np.nan
        forecast_date = match.group(5) or np.nan
        return pd.Series({'industry_cnpj': industry_id, 'industry': industry, 'cnpj': id, 'description': description, 'forecast_date': forecast_date})
    else:
        return pd.Series({'industry_cnpj': np.nan, 'industry': np.nan, 'cnpj': np.nan, 'description': np.nan, 'forecast_date': np.nan})

# COMMAND ----------

# Joining the title dataframe with the other columns of the response
def concatenate_task_title_items(df):
    title_df = df.apply(extract_task_title_items, axis=1)
    final_df = pd.concat([df.drop(columns=['title']), title_df], axis=1)
    return final_df

# COMMAND ----------

tasks = concatenate_task_title_items(tasks)

# COMMAND ----------

# Data wrangling on columns
tasks['industry'] = tasks['industry'].str.strip()
tasks['createdBy'] = tasks['createdBy'].apply(lambda x: x['user']['id'])
tasks['completedBy'] = tasks['completedBy'].apply(lambda x: x['user']['id'] if x else None)
tasks['assignments'] = tasks['assignments'].apply(lambda x: list(x.keys())[0] if len(list(x.keys()))>0 else None)
tasks.loc[tasks['forecast_date'] == 'DD/MM/AAAA', 'forecast_date'] = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Create an association table for tasks and categories applied

# COMMAND ----------

task_categories_data = []

for index, row in tasks.iterrows():
    task_id = row['id']
    applied_categories = row['appliedCategories']

    for category_code, applied in applied_categories.items():
        if applied:
            category_id = category_code
            task_categories_data.append({'task_id': task_id, 'category_id': category_id})

task_categories = pd.DataFrame(task_categories_data)

# COMMAND ----------

# Removing appliedCategories column from tasks dataframe
tasks = tasks.drop('appliedCategories', axis=1)

# COMMAND ----------

#Dropping the example task row
tasks = tasks[tasks['id'] != (dbutils.widgets.get('example_task_id'))]

# COMMAND ----------

# Writing the dataframes created to tables in sandbox database
planner_task_categories = spark.createDataFrame(task_categories).write.format('delta').mode('overwrite').saveAsTable('sandbox.planner_task_categories')
planner_tasks = spark.createDataFrame(tasks).write.format('delta').mode('overwrite').saveAsTable('sandbox.planner_tasks')

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Get all Buckets in a Planner plan

# COMMAND ----------

url_buckets = f'https://graph.microsoft.com/v1.0/planner/plans/{plan_id}/buckets'

# COMMAND ----------

res_buckets = requests.get(url_buckets, headers=headers)

# COMMAND ----------

buckets = pd.DataFrame(res_buckets.json()['value'])

# COMMAND ----------

# Selecting the columns wanted
buckets = buckets[['planId', 'id', 'name']]

# COMMAND ----------

# Writing dataframe to a table
planner_buckets = spark.createDataFrame(buckets).write.format('delta').mode('overwrite').saveAsTable('sandbox.planner_buckets')

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Get Planner Task details

# COMMAND ----------

task_details = []
for index, row in tasks.iterrows():
    task_id = row['id']
    url_details = f'https://graph.microsoft.com/v1.0/planner/tasks/{task_id}/details'
    res_details = requests.get(url_details, headers=headers).json()['description']
    task_details.append({'id': task_id, 'description': res_details})

# COMMAND ----------

details_raw = pd.DataFrame(task_details)

# COMMAND ----------

# Using regex to extract the details from the description and writing to a dataframe
def extract_details(df):
    description = df['description'].replace('\n', '').replace('\r', '').replace('\xa0', '')
    pattern = r'ID CRM:\s*(.*?)[\s;]*Layout:\s*(.*?)[\s;]*ERP:\s*(.*?)[\s;]*SETUP:[\sR$]*([\d.,]*)?(.*?)MRR:[\sR$]*([\d.,]*)?.*'
    match = re.match(pattern, description)
    
    if match:
        crm = match.group(1) or np.nan
        layout = match.group(2) or np.nan
        erp = match.group(3) or np.nan
        setup = match.group(4) or np.nan
        mrr = match.group(6) or np.nan
        return pd.Series({'id_crm': crm, 'layout': layout, 'erp': erp, 'setup': setup, 'mrr': mrr})
    else:
        return pd.Series({'id_crm': np.nan, 'layout': np.nan, 'erp': np.nan, 'setup': np.nan, 'mrr': np.nan})

# COMMAND ----------

# Concatenating details to the original dataframe
def concatenate_details(df):
    details_df = df.apply(extract_details, axis=1)
    final_df = pd.concat([df.drop(columns=['description']), details_df], axis=1)
    return final_df

# COMMAND ----------

details = concatenate_details(details_raw)

# COMMAND ----------

# Writing dataframe to a table
planner_task_details = spark.createDataFrame(details).write.format('delta').mode('overwrite').saveAsTable('sandbox.planner_task_details')

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Get Group users

# COMMAND ----------

group_id = dbutils.widgets.get('group_id')
url_users = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members"

# COMMAND ----------

res_users = requests.get(url_users, headers=headers)

# COMMAND ----------

users = pd.DataFrame(res_users.json()['value'])

# COMMAND ----------

# Selecting only the required columns
users = users[['id', 'givenName', 'surname', 'jobTitle', 'officeLocation', 'mail']]

# COMMAND ----------

# Writing users dataframe to a table
planner_users = spark.createDataFrame(users).write.format('delta').mode('overwrite').saveAsTable('sandbox.planner_users')
