# Serverless ERP Data Lakehouse

[![GitOps CI/CD](https://github.com/zulpham/serverless-erp-lakehouse/actions/workflows/deploy.yml/badge.svg)](https://github.com/zulpham/serverless-erp-lakehouse/actions)

*Read this in other languages: [English](README.md), [Bahasa Indonesia](README-id.md)*

## Project Description
The *Serverless ERP Data Lakehouse* architecture is designed to extract data from OData v4 endpoints, process it functionally, and store it in Amazon S3 using a *Medallion Multi-Tier Storage* pattern. This infrastructure is 100% private, AES-256 (SSE-S3) encrypted, and utilizes automated CI/CD with *Zero-Trust* OIDC integration.

Beyond relying on batch processing, this Lakehouse is also integrated with an **Enterprise AI Text-to-SQL Engine**, enabling direct *Natural Language* query interactions against Apache Iceberg data using Amazon Bedrock and a Streamlit analytics interface.

## Infrastructure & Data Architecture

### 1. Storage Layer (Amazon S3 & Iceberg)
Storage utilizes the Medallion architecture (*Bronze* for transient staging, *Silver/Gold* for ACID warehouse tables).
* **S3 Athena Results:** Stores Athena query execution results with an automated lifecycle policy (7-day expiration).
* **Iceberg Tables:** Integrated in the *Silver/Gold Layer* using AWS Glue Data Catalog.

### 2. Compute Layer (AWS Lambda)
* **Ingestion Worker & SQL Micro-Dispatcher:** Functional Lambdas that manage ingestion and SQL template rendering.

### 3. Analytics Layer & AI Engine
This project prioritizes the delivery of intelligent business insights (*Business Intelligence*) with a dedicated **LakehouseAIEngine** module:
* **Two-Pass AI Strategy (Amazon Bedrock):**
  1.  **Pass 1 (Text-to-SQL):** Translates natural language into Trino SQL queries using the *Amazon Titan Text Express* model. Adapts to dynamic schema reflection from AWS Glue Catalog (cached for 1 hour in memory).
  2.  **Pass 2 (Contextual Synthesis):** Analyzes tabular results to generate an executive summary.
* **Lean Execution Engine:** Free from Pandas dependencies. Results are executed synchronously in Athena (L3 Cache with 24-hour Result Reuse) and processed using the *Rust-backed* Polars format for high RAM efficiency.
* **SQLGlot Security Guardrails:** AST (*Abstract Syntax Tree*) level security blocks 100% of mutation query attempts (DML/DDL like `INSERT`, `DROP`, `DELETE`) and automatically injects a `LIMIT 100` constraint into AI-generated queries.
* **FinOps Control:** Uses a dedicated AWS Athena Workgroup (`lakehouse_workgroup`) that automatically cuts off costs if data scanning per query exceeds 10 GB.

### 4. Interactive Analytics Dashboard (Streamlit)
* Features a multi-tab Streamlit analytics UI providing an *Executive Summary*, *Plotly Interactive Charts*, *Polars* tabular review, and *SQL AST* audit logs.

### 5. Orchestration, Metastore, & CI/CD Pipeline
* **AWS Step Functions & Glue Data Catalog:** Orchestrates ETL/ELT transformations and registers schemas to the metastore.
* **GitOps CI/CD (GitHub Actions):** 
  A two-stage automated delivery system (*Quality Gate & Infrastructure Deployment*):
  1.  **Quality Gate:** Performs dynamic Python AST compilation and static analysis (*Flake8 linting*).
  2.  **Zero-Trust OIDC Deployment:** Executes `terraform plan` and `terraform apply` infrastructure provisioning instructions after OIDC authentication is validated without long-lived keys. Lambda layers are prepared on the manylinux compute platform to maintain Rust binary compatibility for Polars.

## Requirements
* **Terraform:** `>= 1.5.0`
* **AWS Provider:** `~> 5.0`
* **Runtime:** Python 3.11 or 3.12 (for the AI engine and Lambda Layer).
* **Analytics/AI Specific Python Dependencies:** `streamlit`, `plotly`, `sqlglot`, `pyathena`, `polars==1.5.0`, `requests`, `urllib3`, and *AWS SDK* (`boto3`).
