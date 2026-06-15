# MS Planner ETL: Databricks Pipeline

## 📌 Overview
This repository contains a data engineering pipeline designed to extract task, bucket, project, and user data from Microsoft Planner using the Microsoft Graph API. The scripts are built to run in a Databricks environment, automating the ingestion and transformation of project management data for downstream analytics.

## 🏗 Architecture & Scripts

Currently, this repository contains two primary ingestion scripts that extract the same underlying data but serve different pipeline methodologies. **Note: These scripts are partially redundant in their extraction phase.**

* **`ingestion-raw.py` (Modular Raw Ingestion):** Designed for a data lake architecture (Bronze Layer). This script uses defined functions to authenticate and extract the raw JSON payloads from the Graph API, saving them directly to the Databricks File System (DBFS) without any modifications or transformations. 
* **`main.py` (Monolithic ETL):** An end-to-end pipeline script. It extracts the same data from the API but immediately applies heavy transformations using `pandas` and regular expressions. It parses complex strings from task titles and descriptions (extracting business metrics like CNPJ, ERP, Setup costs, and MRR) and writes the cleaned data directly into Delta tables (`sandbox.*`).

## 🚀 Getting Started

### Prerequisites
* Databricks environment (scripts are formatted as Databricks Notebooks).
* Microsoft Azure Active Directory app registration with Graph API permissions.
* Required Python libraries: `msal`, `pandas`, `requests`, `numpy`.

### Execution Variables
Both scripts utilize `dbutils.widgets.get()` to retrieve environment variables dynamically within Databricks. You will need to pass the following parameters:
* `client_id`: Azure AD Application Client ID.
* `client_credential`: Azure AD Application Client Secret.
* `tennant_id`: Azure AD Tenant ID.
* `plan_id`: The specific MS Planner Plan ID.
* `group_id`: The associated Microsoft 365 Group ID.
* `example_task_id` *(main.py only)*: ID of a dummy task to be filtered out during transformation.

### Running the Pipeline
Depending on your desired output:
1.  **For raw data extraction:** Run `ingestion-raw.py` to land JSON files in your `/Raw/` workspace directory.
2.  **For transformed Delta tables:** Run `main.py` to extract, clean, and write structured tables to your `sandbox` database.

## 🛠 Next Steps & Optimization
Future efforts to further develop and improve engineering could begin with refactoring the scripts to eliminate redundancy between them and make them more modular. That way it's possible to develop a strict Medallion architecture (creating a single notebook for each layer - Bronze, Silver and Gold).
