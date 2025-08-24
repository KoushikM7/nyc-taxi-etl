
# NYC Taxi ETL Pipeline 🚖

\<p align="center"\>
\<img src="[https://komarev.com/ghpvc/?username=koushikm7\&label=Profile%20views\&color=0e75b6\&style=flat](https://komarev.com/ghpvc/?username=koushikm7&label=Profile%20views&color=0e75b6&style=flat)" alt="Profile Views"/\>
\</p\>

-----

## 🚀 Project Overview

The **NYC Taxi ETL Pipeline** is a **full-fledged data engineering project** designed to process, clean, and analyze large-scale taxi trip datasets using **PySpark**, **Airflow**, and **AWS cloud services**.

This pipeline simulates a **real-world ETL workflow**, suitable for analytics, business intelligence dashboards, and scalable cloud deployments. It also demonstrates **end-to-end data handling**, from raw input data to processed outputs ready for visualization or machine learning.

-----

## 🛠 Tech Stack

  - **Python**: Core scripting
  - **PySpark**: Distributed data processing for large datasets
  - **Airflow**: Workflow orchestration and scheduling
  - **AWS**: S3 (storage), Redshift (warehouse), Lambda (optional serverless tasks)
  - **React**: Optional dashboard front-end for metrics
  - **Data Visualization**: QuickSight, Matplotlib, Pandas plotting

-----

## 📝 Project Structure

```
nyc-taxi-etl/
├── src/ # ETL scripts: extract, transform, load
├── dags/ # Airflow DAGs for scheduling
├── cloud/ # AWS integration scripts
├── notebooks/ # Exploratory notebooks and analysis
├── data/ # Sample datasets and lookup tables
├── README.md
├── requirements.txt
└── .gitignore
```

-----

## ⚡ Pipeline Features

1.  **Extract**

      - Reads raw **Parquet** files from NYC Taxi data
      - Supports multiple months of data for scalability

2.  **Transform**

      - Cleans null or inconsistent values
      - Adds derived columns: `total_journey` and `tips`
      - Joins with lookup tables (e.g., taxi zones) for enriched analytics

3.  **Load**

      - Writes cleaned and processed datasets back to **Parquet or CSV**
      - Optionally uploads to **AWS S3** or loads into **Redshift**

4.  **Scheduling**

      - Airflow DAG automates daily or monthly ETL runs
      - Monitors pipeline success, failures, and retries

5.  **Analytics-ready**

      - Outputs can be directly used for **QuickSight dashboards** or **Python visualizations**
      - Supports downstream machine learning or predictive analytics

-----

## 🔧 How to Run Locally

1.  Clone the repository:

    ```bash
    git clone https://github.com/koushikm7/nyc-taxi-etl.git
    cd nyc-taxi-etl
    ```

2.  Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3.  Set up your local PostgreSQL database.

4.  Create a `.env` file in the root directory and add the following environment variables for the PostgreSQL connection:

    ```bash
    POSTGRES_URL=jdbc:postgresql://localhost:5432/taxi
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=postgres
    POSTGRES_JAR=C:\hadoop\postgresql-42.7.7.jar
    ```

    **Note:** The `POSTGRES_JAR` path must point to the PostgreSQL JDBC driver file on your local system.

5.  Run the main Python script:

    ```bash
    python main.py
    ```
