# Serverless ERP Data Lakehouse

[![GitOps CI/CD](https://github.com/zulpham/serverless-erp-lakehouse/actions/workflows/deploy.yml/badge.svg)](https://github.com/zulpham/serverless-erp-lakehouse/actions)

*Baca dokumen ini dalam bahasa lain: [English](README.md), [Bahasa Indonesia](README-id.md)*

## Deskripsi Proyek
Arsitektur *Serverless ERP Data Lakehouse* ini dirancang untuk mengekstraksi data dari *endpoint* OData v4, memprosesnya secara fungsional, dan menyimpannya di Amazon S3 dengan pola *Medallion Multi-Tier Storage*. Infrastruktur ini 100% *private*, dienkripsi dengan AES-256 (SSE-S3), dan menggunakan CI/CD terotomasi dengan integrasi *Zero-Trust* OIDC.

Selain mengandalkan pemrosesan batch, Lakehouse ini juga terintegrasi dengan **Enterprise AI Text-to-SQL Engine**, yang memungkinkan interaksi kueri *Natural Language* langsung terhadap data Apache Iceberg menggunakan Amazon Bedrock dan antarmuka analitik Streamlit.

## Arsitektur Infrastruktur & Data

### 1. Storage Layer (Amazon S3 & Iceberg)
Penyimpanan menggunakan arsitektur Medallion (*Bronze* untuk staging transien, *Silver/Gold* untuk tabel *ACID warehouse*).
* **S3 Athena Results:** Menyimpan hasil *query* eksekusi Athena dengan *lifecycle policy* otomatis (7 hari penghapusan).
* **Tabel Iceberg:** Terintegrasi di *Silver/Gold Layer* menggunakan AWS Glue Data Catalog.

### 2. Compute Layer (AWS Lambda)
* **Ingestion Worker & SQL Micro-Dispatcher:** Lambda fungsional yang mengatur *ingestion* dan *rendering* templat SQL. 

### 3. Analytics Layer & AI Engine
Proyek ini mengutamakan penyajian wawasan bisnis cerdas (*Business Intelligence*) dengan modul **LakehouseAIEngine** terdedikasi:
* **Two-Pass AI Strategy (Amazon Bedrock):**
  1.  **Pass 1 (Text-to-SQL):** Menerjemahkan bahasa alami ke kueri Trino SQL menggunakan model *Amazon Titan Text Express*. Menyesuaikan *schema reflection* dinamis dari AWS Glue Catalog (di-_cache_ selama 1 jam di memori).
  2.  **Pass 2 (Contextual Synthesis):** Menganalisis hasil tabular untuk menghasilkan ringkasan eksekutif (*executive summary*).
* **Lean Execution Engine:** Bebas dari dependensi Pandas. Hasil dieksekusi secara sinkronus di Athena (L3 *Cache* dengan 24 jam _Result Reuse_) dan diproses menggunakan format *Rust-backed* Polars untuk efisiensi RAM tinggi.
* **SQLGlot Security Guardrails:** Keamanan tingkat AST (*Abstract Syntax Tree*) memblokir 100% upaya kueri mutasi (DML/DDL seperti `INSERT`, `DROP`, `DELETE`) dan secara otomatis menyuntikkan batasan `LIMIT 100` ke kueri yang dihasilkan AI.
* **FinOps Control:** Menggunakan AWS Athena Workgroup khusus (`lakehouse_workgroup`) yang secara otomatis memotong biaya bila pemindaian data per kueri menembus 10 GB.

### 4. Interactive Analytics Dashboard (Streamlit)
* Memiliki UI analitik Streamlit multi-tab yang menyediakan *Executive Summary*, *Plotly Interactive Charts*, peninjauan tabular *Polars*, dan log audit *SQL AST*.

### 5. Orchestration, Metastore, & CI/CD Pipeline
* **AWS Step Functions & Glue Data Catalog:** Mengorkestrasi transformasi ETL/ELT dan mendaftarkan *schema* ke *metastore*.
* **GitOps CI/CD (GitHub Actions):** 
  Sistem pengiriman dua tahap (*Quality Gate & Infrastructure Deployment*):
  1.  **Quality Gate:** Melakukan kompilasi AST Python secara dinamis dan statis (*Flake8 linting*).
  2.  **Zero-Trust OIDC Deployment:** Mengeksekusi instruksi pembentukan infrastruktur `terraform plan` dan `terraform apply` setelah autentikasi OIDC disahkan tanpa long-lived keys. Lambda layer disiapkan di platform komputasi manylinux untuk menjaga kompatibilitas _Rust binary_ pada Polars.

## Prasyarat
* **Terraform:** `>= 1.5.0`
* **AWS Provider:** `~> 5.0`
* **Runtime:** Python 3.11 atau 3.12 (untuk *engine AI* dan Lambda *Layer*).
* **Dependensi Python Khusus Analytics/AI:** `streamlit`, `plotly`, `sqlglot`, `pyathena`, `polars==1.5.0`, `requests`, `urllib3`, dan *AWS SDK* (`boto3`).
