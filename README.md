# Snowflake Snowpark Brainstorming
# [uat] CI/CD test commit

This project is set up for experimenting with Snowflake and Snowpark in an isolated environment. All dependencies required to connect to Snowflake will be installed. Docker will be used for isolation.

## Features
- Python environment
- Snowflake connector and Snowpark
- Docker-based isolation

## Setup Instructions
1. Build the Docker container:
   ```bash
   docker build -t snowflake_snowpark_brainstorming .
   ```
2. Run the container:
   ```bash
   docker run -it --rm snowflake_snowpark_brainstorming
   ```
3. Add your Snowflake credentials in the appropriate config file or environment variables.

## Requirements
- Docker
- Python 3.8+
- snowflake-connector-python
- snowflake-snowpark-python

## Usage
Start by editing `main.py` to connect and interact with Snowflake using Snowpark.
