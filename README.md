# Currency Exchange Rate Pipeline

An automated ETL pipeline built on Apache Airflow.

## What it does
Every hour, it retrieves currency exchange rates (USD → EUR, GBP, JPY, ILS, CHF) 
from a public API and stores them in PostgreSQL.

## Technology stack
- Apache Airflow 2.8
- PostgreSQL 15
- Docker Compose
- Python 3.11

## Architecture
extract → transform → validate → load

## How to run
1. Clone the repository
2. Create a .env file (example in .env.example)
3. docker compose up -d
4. Open localhost:8080 (admin/admin)

Translated with DeepL.com (free version)