from fastapi import FastAPI
from dotenv import load_dotenv
import snowflake.connector
import os

load_dotenv()

app = FastAPI()

@app.get("/health")
async def health_check():
    # Test connection to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA_BRONZE")
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()
    cursor.close()
    conn.close()
    return {"status": "healthy", "snowflake_version": version}


@app.get("/")
async def filter_dataset():
    pass