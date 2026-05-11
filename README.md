# databricks-_mortgage_lakehousepipeline

# Lakehouse Pipeline — Mortgage Loan Analytics

A data pipeline that processes US mortgage loan data through three structured Delta Lake layers — raw, refined, and curated — using Apache Spark on Databricks, with Apache Airflow handling orchestration and scheduling.


#What This Project Does

Raw mortgage data comes in as a CSV. The pipeline picks it up, runs it through a cleaning and validation stage, and produces a final analytics-ready dataset that can be used to study loan approval patterns by credit score, income, state, and loan type. Airflow schedules and monitors the whole thing daily.

# Tech Stack

- **Databricks** — runs the Spark notebooks and stores Delta tables
- **Apache Spark / PySpark** — handles all data transformation
- **Delta Lake** — storage format with ACID transactions across all three layers
- **Apache Airflow** — schedules and orchestrates the pipeline
- **Docker** — runs Airflow locally in containers
- **Python** — transformation logic, quality checks, and DAG definition


# Project Structure
mortgage-lakehouse-pipeline/
├── databricks/
│   ├── 01_raw_layer.py           # Ingest source data into Delta
│   ├── 02_refined_layer.py       # Clean, validate, quality checks
│   └── 03_curated_layer.py       # Final features for loan analysis
├── dags/
│   └── mortgage_pipeline.py      # Airflow DAG definition
├── docker-compose.yml            # Airflow + Postgres setup
├── requirements.txt
└── README.md



## Pipeline Overview
Source CSV (mortgage data)

01 — Raw Layer (Bronze)
- Ingests the CSV as-is into a Delta table
- No transformations — preserves the original source
- Adds ingestion timestamp and source metadata columns

Data Quality Gate
- Checks for nulls in critical columns (credit score, income, DTI)
- Flags invalid loan amounts and unsupported loan terms
- Pipeline stops here if checks fail

02 — Refined Layer (Silver)
- Fills nulls using median values
- Filters out invalid records
- Standardizes column types
- Adds human-readable action label (Approved / Denied / Withdrawn)

03 — Curated Layer (Gold)
- Adds credit band (Poor / Fair / Good / Excellent)
- Adds income band (Low / Medium / High / Very High)
- Computes loan-to-income ratio
- Final table ready for loan approval analysis

Airflow DAG
- Runs all three layers in sequence daily
- Manages task dependencies
- Logs each stage result


## Setup and Running the Project

### Prerequisites

- Databricks account (Free Edition works fine)
- Docker Desktop installed on your machine
- Python 3.8+


### Step 1 — Clone the repo

git clone https://github.com/anusham2512/mortgage-lakehouse-pipeline.git
cd mortgage-lakehouse-pipeline



### Step 2 — Run the Databricks notebooks

Log into your Databricks workspace and run the notebooks in order:

Notebook 1 — Raw layer

Notebook 2 — Refined layer

Notebook 3 — Curated layer

### Step 3 — Start Airflow

```bash
docker-compose up -d postgres
sleep 10
docker-compose run --rm airflow-init
docker-compose up -d
```

Create the admin user:

### Step 4 — Open the Airflow UI

Go to 'http://localhost:8081' and log in with credentials.


### Step 5 — Trigger the DAG

Search for 'mortgage_lakehouse_pipeline', click the play button, and select 'Trigger DAG'.
