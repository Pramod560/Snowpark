import configparser
import snowflake.snowpark as snowpark


def get_snowflake_session(config_path="snowflake_snowpark_brainstorming/config.ini"):
    config = configparser.ConfigParser()
    config.read(config_path)
    creds = config["snowflake"]
    session = snowpark.Session.builder.configs({
        "account": creds["account"],
        "user": creds["user"],
        "password": creds["password"],
        "role": creds["role"],
        "warehouse": creds["warehouse"],
        "database": creds["database"],
        "schema": creds["schema"]
    }).create()
    return session
