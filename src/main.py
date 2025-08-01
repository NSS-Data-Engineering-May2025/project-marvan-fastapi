from fastapi import FastAPI, Query, HTTPException
from typing import Optional
import snowflake.connector
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv()

app = FastAPI()

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="MARVAN_DB",
        schema="PRODUCTION_GOLD",
        login_timeout=10
    )

def run_query(query: str):
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        logging.info("Executing query:\n%s", query)
        cursor.execute(query)
        result = cursor.fetchall()
    except Exception as e:
        logging.exception("Database query failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    if not result:
        raise HTTPException(status_code=404, detail="No dataset found for given parameters")

    return [
        {
            "country": row[0],
            "dataset_name": row[1],
            "description": row[2],
            "last_updated": row[3].strftime('%Y-%m-%d') if row[3] else None,
            "data": row[4]
        }
        for row in result
    ]

@app.get("/")
def get_data(
    country: Optional[str] = Query(None, description="Filter by country name"),
    keyword: Optional[str] = Query(None, description="Keyword in dataset name or description"),
    last_updated: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(100, description="Limit number of results (default 100, max 500)")
):
    filters = []
    if country:
        filters.append(f"LOWER(country) = LOWER('{country}')")
    if keyword:
        filters.append(f"(dataset_name ILIKE '%{keyword}%' OR description ILIKE '%{keyword}%')")
    if last_updated:
        filters.append(f"TO_CHAR(last_updated, 'YYYY-MM-DD') = '{last_updated}'")

    if limit > 500:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 500 rows.")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    query = f"""
        SELECT country, dataset_name, description, last_updated, data
        FROM COVID_DATA_SUMMARY
        {where_clause}
        LIMIT {limit}
    """

    return run_query(query)
