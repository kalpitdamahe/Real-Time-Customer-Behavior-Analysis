# Real-Time Customer Behavior Analysis Pipeline



![img8](https://github.com/user-attachments/assets/16607196-936b-482d-bd40-7ddadfc3d187)



📈 **Overview**

This project is a real-time data pipeline built to analyze customer behavior as events happen.
It captures events from a live application, processes them in real-time, and makes the data available for analytics and reporting.

The system uses modern technologies like Kafka, Spark, InfluxDB, Databricks, Snowflake, Docker, and Apache Airflow.





# 🧱 Technologies Used

This project utilizes the following technologies:

| **Technology**              | **Purpose**                                    |
|-----------------------------|------------------------------------------------|
| **Flask**                   | Used for developing web applications using python.                      |
| **Kafka**                   | Real-time message broker for transmitting events.                      |
| **Spark (Structured Streaming)** | Processes Kafka streams (Real-time data processing)              |
| **InfluxDB**                | Time-series database, captures time-series metrics                          |
| **Databricks**              | Data engineering and transformations, Transforms and enriches data before warehousing.         |
| **Snowflake**               | Data warehouse for analytics, Final destination for analytical queries and dashboards.                  |
| **Airflow**                 | Workflow orchestration, Manages and schedules batch jobs and ETL workflows.                        |
| **Docker**                  | Containerization of services, Containerizes and orchestrates services for local deployment.                  |

## 🛠️ Architecture

**Components:**

- **Application Server (Flask):**  
  Simulates real-time customer behavior and generates streaming data.

- **Apache Kafka:**  
  Acts as the real-time messaging layer, decoupling the event producers and consumers.

- **Apache Spark (Structured Streaming):**  
  Consumes data from Kafka in real-time, performs transformations, and writes it to downstream systems.

- **InfluxDB:**  
  Stores time-series data for quick time-based analysis and dashboarding.

- **Databricks:**  
  Further processes, aggregates, and prepares the data for analytical use cases.

- **Snowflake:**  
  Final storage for structured data, ready for complex analytics and reporting.

- **Apache Airflow:**  
  Orchestrates and schedules batch jobs across the pipeline.

- **Docker:**  
  Containerizes services like Kafka, InfluxDB, and Spark for a reproducible local development setup.

	## 📦 Project Setup

### Prerequisites

- Docker & Docker Compose  
- Python 3.8+  
- Databricks Account  
- Snowflake Account

  ## 🐳 Docker Links

Below are the official and customized Docker sources used to spin up various components of this pipeline:

- **Kafka (v7.7.1):**  
  [Confluent Platform Docker GitHub Repo](https://github.com/confluentinc/cp-all-in-one)  
  → Using: [`cp-all-in-one-kraft` (7.7.1-oauth-fix)](https://github.com/confluentinc/cp-all-in-one/tree/7.7.1-oauth-fix/cp-all-in-one-kraft)

- **Apache Airflow (v2.10.2):**  
  [Airflow Official Docker Compose Guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)  
  → Using: [Airflow 2.10.2 Docker Setup](https://airflow.apache.org/docs/apache-airflow/2.10.2/howto/docker-compose/index.html)

- **InfluxDB (v2):**  
  [InfluxDB Docker Compose Guide](https://docs.influxdata.com/influxdb/v2/install/use-docker-compose/)


## 📈 Use Cases

- Live customer activity dashboards  
- Funnel & drop-off analysis  
- User behavior segmentation  
- Retention & churn prediction

✨ Credits
Built with ❤️ by Kalpit Damahe
Inspired by real-time customer analytics architectures used in production systems.
