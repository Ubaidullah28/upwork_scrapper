import psycopg2
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

def get_search_queries_from_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT "id", "search_criteria"
        FROM "published"."practice"
        WHERE "status" = 0
        ORDER BY "id" ASC
        LIMIT 1
    ''')
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return {"id": row[0], "search_criteria": row[1]}
    return None

def read_last_scrape_time(query):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT "search_criteria_time"
        FROM "published"."practice"
        WHERE LOWER("search_criteria") = LOWER(%s)
        LIMIT 1
    ''', (query,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row and row[0]:
        return row[0]
    return None

def update_scrape_time(query):
    current_time = datetime.now() - timedelta(minutes=2)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        UPDATE "published"."practice"
        SET "search_criteria_time" = %s
        WHERE LOWER("search_criteria") = LOWER(%s)
    ''', (current_time, query))
    conn.commit()
    cur.close()
    conn.close()

def get_source_info():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT source_id, source_name
        FROM published.source
        WHERE source_id = 1
        LIMIT 1
    ''')
    row = cur.fetchone()
    cur.close()
    conn.close()
    return {"source_id": row[0], "source_name": row[1]} if row else None


def insert_jobs_into_public_job(df, source_info, search_criteria):
    if source_info is None:
        print("No source info found")
        return

    source_id = source_info["source_id"]
    source_name = source_info["source_name"]

    print("Inserting 1 job run record into public.job...")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute('''
            INSERT INTO public.job (job, source_id, params)
            VALUES (%s, %s, %s)
        ''', (source_name, source_id, search_criteria))

        print(f"Inserted job run for source '{source_name}' and search '{search_criteria}'")
    except Exception as e:
        print(f"Error inserting job run record: {str(e)}")
    finally:
        conn.commit()
        cur.close()
        conn.close()



