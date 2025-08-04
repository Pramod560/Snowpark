import configparser
import snowflake.snowpark as snowpark
import snowflake.connector


def get_snowflake_session(config_path="config.ini"):
    config = configparser.ConfigParser()
    config.read(config_path)
    config_section = config["snowflake"]
    connection_parameters = {
        "account": config_section["account"],
        "user": config_section["user"],
        "password": config_section["password"],
        "database": config_section["database"],
        "schema": config_section["schema"],
        "warehouse": config_section["warehouse"],
        "role": config_section["role"]
    }
    session = snowpark.Session.builder.configs(connection_parameters).create()
    return session

def get_snowflake_session2(config_path="config.ini"):
    config = configparser.ConfigParser()
    config.read(config_path)
    config_section = config["snowflake"]
    connection_parameters = {
        "account": config_section["account"],
        "user": config_section["user"],
        "password": config_section["password"],
        "database": config_section["database"],
        "schema": config_section["schema"],
        "warehouse": config_section["warehouse"],
        "role": config_section["role"]
    }
    session = snowflake.connector.connect(**connection_parameters)
    return session

