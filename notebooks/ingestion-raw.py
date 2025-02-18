# Databricks notebook source
!pip install msal

import requests
import json
from msal import ConfidentialClientApplication

# COMMAND ----------

def get_auth_token(client_id, client_credential, tennant_id):

    authority = f'https://login.microsoftonline.com/{tennant_id}'
    scope = ['https://graph.microsoft.com/.default']

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

    return headers

# COMMAND ----------

def get_plan_data(plan_id, headers):

    url_plan_details = f'https://graph.microsoft.com/v1.0/planner/plans/{plan_id}/details'
    url_buckets = f'https://graph.microsoft.com/v1.0/planner/plans/{plan_id}/buckets'
    url_tasks = f'https://graph.microsoft.com/v1.0/planner/plans/{plan_id}/tasks'
    
    res_plan_details = requests.get(url_plan_details, headers=headers)
    plan_details_json = json.dumps(res_plan_details.json())

    res_plan_buckets = requests.get(url_buckets, headers=headers)
    plan_buckets_json = json.dumps(res_plan_buckets.json())

    res_plan_tasks = requests.get(url_tasks, headers=headers)
    plan_tasks_json = res_plan_tasks.json()['value']

    page = 1
    while '@odata.nextLink' in res_plan_tasks.json().keys():
        res_plan_tasks = requests.get(res_plan_tasks.json()['@odata.nextLink'], headers=headers)
        plan_tasks_json_2 = res_plan_tasks.json()['value']
        plan_tasks_json.extend(plan_tasks_json_2)
        page += 1
    else:
        print(f'Pagination completed for tasks: {page} pages were successfully merged into a single list of dictionaries! In total, there are {len(plan_tasks_json)} tasks!')
        plan_tasks_json = json.dumps(plan_tasks_json)

    return plan_details_json, plan_buckets_json, plan_tasks_json


# COMMAND ----------

def get_task_data(plan_tasks_json, headers):

    task_details = []

    for task in list(json.loads(plan_tasks_json)):
        task_id = task['id']
        url_details = f'https://graph.microsoft.com/v1.0/planner/tasks/{task_id}/details'
        res_details = requests.get(url_details, headers=headers).json()
        task_details.append(res_details)

    return json.dumps(task_details)

# COMMAND ----------

def get_users_data(group_id, headers):

    url_users = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members"

    res_users = requests.get(url_users, headers=headers)
    users_json = json.dumps(res_users.json())

    return users_json

# COMMAND ----------

def write_data(data, path):
    print(f'Writing data to {path}...')
    dbutils.fs.put(path, data, overwrite=True)

# COMMAND ----------

# Defining app information
client_id = dbutils.widgets.get('client_id')
client_credential = dbutils.widgets.get('client_credential')
tennant_id = dbutils.widgets.get('tennant_id')

# Defining plan and group information
plan_id = dbutils.widgets.get('plan_id')
group_id = dbutils.widgets.get('group_id')

# Defining saving paths
plan_details_path = '/Workspace/Sandbox/Planner/Raw/plan_details.json'
plan_buckets_path = '/Workspace/Sandbox/Planner/Raw/plan_buckets.json'
plan_tasks_path = '/Workspace/Sandbox/Planner/Raw/plan_tasks.json'
task_details_path = '/Workspace/Sandbox/Planner/Raw/task_details.json'
users_path = '/Workspace/Sandbox/Planner/Raw/users.json'

# COMMAND ----------

headers = get_auth_token(client_id, client_credential, tennant_id)

# COMMAND ----------

plan_details_json, plan_buckets_json, plan_tasks_json = get_plan_data(plan_id, headers)
task_details = get_task_data(plan_tasks_json, headers)
users = get_users_data(group_id, headers)

# COMMAND ----------

write_data(plan_details_json, plan_details_path)
write_data(plan_buckets_json, plan_buckets_path)
write_data(plan_tasks_json, plan_tasks_path)
write_data(task_details, task_details_path)
write_data(users, users_path)