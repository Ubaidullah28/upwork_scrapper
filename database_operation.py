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

def insert_raw_json_data(raw_json_data):

    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get the maximum ID from public.job table
    cur.execute('''
        SELECT MAX(id) FROM public.job
    ''')
    
    result = cur.fetchone()
    if result and result[0] is not None:
        job_id = result[0]
        print(f"Retrieved latest job_id from public.job: {job_id}")
    else:
        print("No records found in public.job table")
        cur.close()
        conn.close()
        return None
    
    # Insert raw JSON data into raw.UpworkDataJson
    cur.execute('''
        INSERT INTO raw."UpworkDataJson" ("JobId", "RawJson")
        VALUES (%s, %s)
    ''', (job_id, raw_json_data))
    
    print(f"Inserted raw JSON data into raw.UpworkDataJson for job_id: {job_id}")
    
    conn.commit()
    cur.close()
    conn.close()
    return job_id

def insert_df_into_staging_lead(df):

    if df.empty:
        print("DataFrame is empty, nothing to insert")
        return None
        
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get the maximum ID from raw.UpworkDataJson table
    cur.execute('''
        SELECT MAX("RawId") FROM raw."UpworkDataJson"
    ''')
    
    result = cur.fetchone()
    if result and result[0] is not None:
        raw_id = result[0]
        print(f"Retrieved latest raw_id from raw.UpworkDataJson: {raw_id}")
    else:
        print("No records found in raw.UpworkDataJson table")
        cur.close()
        conn.close()
        return None
    
    # Insert each row from DataFrame into staging.lead
    inserted_count = 0
    for index, row in df.iterrows():
        # Map DataFrame columns to staging.lead columns
        cur.execute('''
            INSERT INTO staging.lead (
                lead_name, 
                "desc", 
                time_posted, 
                link, 
                raw_id,
                budget_type,
                hour_rate_low,
                hour_rate_high,
                fix_price
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            row.get('Title', 'N/A'),                    # lead_name
            row.get('Description', 'N/A'),              # desc  
            row.get('Posted Time'),                     # time_posted
            row.get('Job Link', 'N/A'),                 # link
            raw_id,                                     # raw_id
            row.get('Budget Type', 'N/A'),              # budget_type
            row.get('Lower Hourly Rate', 'N/A'),        # hour_rate_low
            row.get('Higher Hourly Rate', 'N/A'),       # hour_rate_high
            row.get('Fixed Price', 'N/A')               # fix_price
        ))
        inserted_count += 1
    
    print(f"Inserted {inserted_count} lead records into staging.lead with raw_id: {raw_id}")
    
    conn.commit()
    cur.close()
    conn.close()
    return raw_id


