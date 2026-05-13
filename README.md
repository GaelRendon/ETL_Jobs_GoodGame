# ETL_Jobs_GoodGame

This repository contains two main parts:

1. Data workflow / pipeline
2. Analytics and exploration

The ETL pipeline extracts game data from Azure PostgreSQL, stages it in Parquet, uploads it to Snowflake, and loads it into the data warehouse. The analytics part includes a Streamlit app and notebook for analyzing player behavior, performance, and sentiment.

## Repository structure

- `docker-compose.yaml` - Airflow and Postgres services for local execution
- `Dockerfile` - container image build configuration
- `.env` - local environment variables used by Airflow and the ETL jobs
- `dags/` - Airflow DAG definitions
- `jobs/` - ETL logic for PostgreSQL staging and Snowflake DW loading
- `analysis/` - analytics code and notebook
- `data_output/` - staging output folder for parquet and intermediate files
- `requirements.txt` - Python dependencies for the ETL and Snowflake integration

## Prerequisites

- Docker
- Docker Compose
- Python 3.11+ (for local analytics or manual Python execution)
- Optional: Streamlit, pandas, plotly if you run the analytics app locally

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-org/ETL_Jobs_GoodGame.git
   cd ETL_Jobs_GoodGame
   ```

2. Verify or update `.env` with your local credentials:

   - `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD`
   - `AIRFLOW_DB_USER` / `AIRFLOW_DB_PASSWORD`
   - `SF_USER`, `SF_PASSWORD`, `SF_ACCOUNT`, `SF_DATABASE`, `SF_SCHEMA`, `SF_WAREHOUSE`
   - `PG_USER`, `PG_PASSWORD`, `PG_HOST`, `PG_DATABASE`

   > Do not commit secrets to version control.

3. Build and start the stack:

   ```bash
   docker compose up --build
   ```

## Run the ETL pipeline

1. Open Airflow UI in your browser:

   - http://localhost:8080

2. Log in using the values from `.env`:

   - Username: `airflow`
   - Password: `airflow`

3. In the Airflow UI, find the DAG named `goodgame_etl_pipeline`.
4. Trigger the DAG manually, or wait for its schedule:

   - Schedule: every 2 hours (`0 */2 * * *`)

### What the DAG does

- `extract_postgres_data`: reads tables from Azure PostgreSQL and writes them to local Parquet under `data_output/`
- `load_incremental_dw`: reads staged Parquet files and loads incremental data into Snowflake

## Run analytics

The analytics section is in `analysis/` and includes a Streamlit app and exploratory notebook.

### Option 1: Run the Streamlit dashboard

1. Install Python dependencies:

   ```bash
   python -m pip install -r requirements.txt
   python -m pip install streamlit pandas plotly
   ```

2. Start the app:

   ```bash
   streamlit run analysis/analityc.py
   ```

3. Open the local Streamlit URL shown in the terminal.

### Option 2: Open the notebook

- Use `analysis/EDA.ipynb` for exploratory analysis and visual inspection in Jupyter.

## Notes

- The ETL jobs use `jobs/postgres_to_stage.py` and `jobs/stage_to_snowflake_dw.py`.
- `dags/goodgame_etl_pipeline.py` defines the pipeline and task order.
- The analytics app connects to Snowflake and reads tables from `GOOD_GAME_DB.GOLD`.

## Useful files

- `dags/goodgame_etl_pipeline.py` - Airflow pipeline definition
- `jobs/postgres_to_stage.py` - extract and stage raw tables
- `jobs/stage_to_snowflake_dw.py` - incremental load into Snowflake
- `analysis/analityc.py` - Streamlit dashboard for sentiment and performance analysis
- `analysis/EDA.ipynb` - exploratory analysis notebook
