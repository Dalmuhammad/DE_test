# Data Engineering Technical Test

This repository contains a simple data pipeline project for data ingestion, data cleaning, and query analysis.

The project can be run using Docker so the environment can be reproduced easily.

To run the project using Docker, use the following command:

docker compose up --build

This command will automatically:
- Start the MySQL database
- Create the required tables
- Run the Python ingestion pipeline
- Perform data cleaning from raw tables to clean tables

If Docker does not run properly on your system, you can run the pipeline manually using the following steps.

1. Run all SQL files located in the "sql_files" subfolder in your MySQL database. This step will create all required tables.

2. Install the required Python dependencies by running the following command:
pip install -r requirements.txt

3. Run the script "pyscript/ingest_raw.py" to ingest the CSV data into the raw tables (for example: customer_address_raw).

4. Run the script "pyscript/cleaning_tables.py" to perform the cleaning process from raw tables into clean tables.

5. To see the result required in Question 2B, you can execute the SQL queries located in the "query_2b" subfolder.

6. The pipeline design is provided as an image file (pipeline_design.jpg) which illustrates the data flow from raw ingestion to clean tables.

Project Structure:
- sql_init        : SQL files for database initialization
- pyscript        : Python scripts for ingestion and cleaning
- query_2b        : SQL queries for question 2B
- pipeline_design : Image showing the pipeline architecture

Technologies used in this project:
- Python
- MySQL
- Docker
