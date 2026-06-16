# MS Planner ETL: Databricks Pipeline

## 📌 Overview
This repository contains a data engineering pipeline designed to extract task, bucket, project, and user data from Microsoft Planner using the Microsoft Graph API. The scripts are built to run in a Databricks environment, automating the ingestion and transformation of project management data for downstream analytics.

## 🏗 Architecture & Scripts

Currently, this repository contains two primary ingestion scripts that extract the same underlying data but serve different pipeline methodologies. **Note: These scripts are partially redundant in their extraction phase.**

* **`ingestion-raw.py` (Modular Raw Ingestion):** Designed for a data lake architecture (Bronze Layer). This script uses defined functions to authenticate and extract the raw JSON payloads from the Graph API, saving them directly to the Databricks File System (DBFS) without any modifications or transformations. 
* **`main.py` (Monolithic ETL):** An end-to-end pipeline script. It extracts the same data from the API but immediately applies heavy transformations using `pandas` and regular expressions. It parses complex strings from task titles and descriptions (extracting business metrics like CNPJ, ERP, Setup costs, and MRR) and writes the cleaned data directly into Delta tables (`sandbox.*`).

* ## 🏢 Business Context & Data Extraction Logic

This project was built to solve a specific data engineering problem: extracting structured business metrics from unstructured project management boards. 

The pipeline processes data from a Microsoft Planner board used to track technical customer implementations and software integrations. Because Planner lacks native custom fields for logging specific metadata—like financial figures or software versions—the operational team relies on strict naming conventions within the task titles and descriptions to record this information. 

The `main.py` script functions as a custom parser. It reads these free-text fields and transforms them into clean, tabular data so that it can be queried and visualized in downstream analytics.

To accurately model the data, the script applies extraction logic based on the following business rules:

### Task Title Parsing
Task titles are formatted to capture high-level account and timeline information. The script parses the title string to extract:
* **Industry & Client Name:** The primary account identifier.
* **CNPJ:** Extracts the national company registry numbers (both for the industry and the client) to ensure accurate cross-referencing with other databases.
* **Task Description:** The core objective of the specific planner card.
* **Forecast Date:** The expected go-live or completion date for the implementation.

### Task Description Parsing
The task descriptions serve as a repository for the technical and financial specifics of each integration. The pipeline scans these text blocks to capture:
* **ID CRM:** The identifier of the sales proposal that links the Planner task back to the CRM system.
* **Layout:** The specific data architecture or integration layout being deployed.
* **ERP:** The Enterprise Resource Planning system that the client is integrating with.
* **Setup:** The initial implementation or setup fee associated with the project.
* **MRR:** The Monthly Recurring Revenue tied to the client's contract.

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
