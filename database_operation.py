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

def get_max_lead_id():

    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute('''
        SELECT MAX(lead_id) FROM staging.lead
    ''')
    
    result = cur.fetchone()
    if result and result[0] is not None:
        max_lead_id = result[0]
        print(f"Retrieved maximum lead_id from staging.lead: {max_lead_id}")
        cur.close()
        conn.close()
        return max_lead_id
    else:
        print("No records found in staging.lead table, returning 0")
        cur.close()
        conn.close()
        return 0

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

def insert_df_into_staging_client(df, max_lead_id_before):
    
    if df.empty:
        print("DataFrame is empty, nothing to insert")
        return 0
        
    print(f"Using max_lead_id_before: {max_lead_id_before}")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get leads with lead_id greater than max_lead_id_before
    cur.execute('''
        SELECT lead_id FROM staging.lead 
        WHERE lead_id > %s
        ORDER BY lead_id
    ''', (max_lead_id_before,))
    
    new_lead_ids = [row[0] for row in cur.fetchall()]
    print(f"Found {len(new_lead_ids)} new lead_ids: {new_lead_ids}")
    
    if not new_lead_ids:
        print("No new leads found to insert client data for")
        cur.close()
        conn.close()
        return 0
    
    # Insert client data for each new lead
    inserted_count = 0
    for i, (index, row) in enumerate(df.iterrows()):
        if i < len(new_lead_ids):
            lead_id = new_lead_ids[i]
            
            cur.execute('''
                INSERT INTO staging.client (
                    client_name, 
                    client_spent, 
                    lead_id,
                    payment_method
                ) VALUES (%s, %s, %s, %s)
            ''', (
                row.get('Client Name', 'N/A'),          # client_name
                row.get('Client Spent', 'N/A'),         # client_spent  
                lead_id,                                 # lead_id
                row.get('Payment Verified/Unverified', 'N/A')  # payment_method
            ))
            inserted_count += 1
    
    print(f"Inserted {inserted_count} client records into staging.client")
    
    conn.commit()
    cur.close()
    conn.close()
    return inserted_count

def insert_df_into_staging_tag(df, max_lead_id_before):
    """
    Insert DataFrame into staging.tag table for leads with lead_id greater than max_lead_id_before
    
    Args:
        df: DataFrame containing tag data
        max_lead_id_before: Maximum lead_id before the current insertion
        
    Returns:
        int: Number of records inserted
    """
    
    if df.empty:
        print("DataFrame is empty, nothing to insert")
        return 0
        
    print(f"Using max_lead_id_before for tags: {max_lead_id_before}")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get leads with lead_id greater than max_lead_id_before
    cur.execute('''
        SELECT lead_id FROM staging.lead 
        WHERE lead_id > %s
        ORDER BY lead_id
    ''', (max_lead_id_before,))
    
    new_lead_ids = [row[0] for row in cur.fetchall()]
    print(f"Found {len(new_lead_ids)} new lead_ids for tags: {new_lead_ids}")
    
    if not new_lead_ids:
        print("No new leads found to insert tag data for")
        cur.close()
        conn.close()
        return 0
    
    # Insert tag data for each new lead
    inserted_count = 0
    for i, (index, row) in enumerate(df.iterrows()):
        if i < len(new_lead_ids):
            lead_id = new_lead_ids[i]
            
            cur.execute('''
                INSERT INTO staging.tag (
                    tag_list, 
                    lead_id
                ) VALUES (%s, %s)
            ''', (
                row.get('Tags', 'N/A'),                 # tag_list
                lead_id                                 # lead_id
            ))
            inserted_count += 1
    
    print(f"Inserted {inserted_count} tag records into staging.tag")
    
    conn.commit()
    cur.close()
    conn.close()
    return inserted_count

import psycopg2
import pandas as pd
from database_operation import get_db_connection

def get_new_leads_data():
    """
    Get all new leads data that don't exist in published.lead using your exact query
    
    Returns:
        pd.DataFrame: DataFrame with new leads data
    """
    conn = get_db_connection()
    
    query = """
    SELECT 
        SL."lead_name",
        SL."desc",
        SL."time_posted",
        SL."link",
        SL."budget_type",
        SL."hour_rate_low",
        SL."hour_rate_high",
        SL."fix_price",
        SL."raw_id",
        SL."lead_id" as staging_lead_id,
        
        SC."client_name",
        SC."client_spent", 
        SC."payment_method",
        
        ST."tag_list"
    FROM 
        "staging"."lead" SL 
    LEFT JOIN 
        "published"."lead" DL 
    ON 
        SL."link" = DL."link"
    LEFT JOIN 
        "staging"."client" SC 
    ON 
        SL."lead_id" = SC."lead_id"
    LEFT JOIN 
        "staging"."tag" ST 
    ON 
        SL."lead_id" = ST."lead_id"
    WHERE 
        DL."link" IS NULL
    ORDER BY SL."lead_id";
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"📋 Found {len(df)} new leads to process")
    return df

def insert_leads_to_published(df):
    """
    Insert leads data into published.lead table
    
    Args:
        df (pd.DataFrame): DataFrame containing leads data
        
    Returns:
        dict: mapping of staging_lead_id to new published_lead_id
    """
    if df.empty:
        print("No leads to insert")
        return {}
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    lead_id_mapping = {}  # staging_lead_id -> published_lead_id
    inserted_count = 0
    
    print(f"🔄 Inserting {len(df)} leads into published.lead...")
    
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO "published"."lead" (
                "lead_name", 
                "desc", 
                "time_posted", 
                "link", 
                "budget_type",
                "hour_rate_low",
                "hour_rate_high", 
                "fix_price"
                
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING "lead_id"
        """, (
            row['lead_name'],
            row['desc'], 
            row['time_posted'],
            row['link'],
            row['budget_type'],
            row['hour_rate_low'],
            row['hour_rate_high'],
            row['fix_price']
            
        ))
        
        new_lead_id = cur.fetchone()[0]
        lead_id_mapping[row['staging_lead_id']] = new_lead_id
        inserted_count += 1
        
        print(f"✅ Inserted lead: {row['lead_name'][:50]}... (New ID: {new_lead_id})")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"🎉 Successfully inserted {inserted_count} leads into published.lead")
    return lead_id_mapping

def insert_clients_to_published(df, lead_id_mapping):
    """
    Insert client data into published.client table based on available lead info.
    
    Args:
        df (pd.DataFrame): DataFrame containing lead-related client data
        lead_id_mapping (dict): mapping of staging_lead_id to published_lead_id
        
    Returns:
        int: number of clients inserted
    """
    if df.empty:
        print("No data to insert")
        return 0

    conn = get_db_connection()
    cur = conn.cursor()

    inserted_count = 0

    print(f"🔄 Inserting client info for {len(df)} leads into published.client...")

    for _, row in df.iterrows():
        staging_lead_id = row['staging_lead_id']
        published_lead_id = lead_id_mapping.get(staging_lead_id)

        if published_lead_id is None:
            print(f"⚠️ Warning: No published lead_id found for staging lead_id {staging_lead_id}")
            continue

        cur.execute("""
            INSERT INTO "published"."client" (
                "client_name", 
                "client_spent", 
                "lead_id",
                "payment_method"
            ) VALUES (%s, %s, %s, %s)
        """, (
            row.get('client_name', None),  # Still insert if available, otherwise NULL
            row.get('client_spent', None),
            published_lead_id,
            row.get('payment_method', None)
        ))

        inserted_count += 1
        print(f"✅ Inserted client info for lead ID: {published_lead_id}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"🎉 Successfully inserted {inserted_count} clients into published.client")
    return inserted_count

def insert_tags_to_published(df, lead_id_mapping):
    """
    Insert tag data into published.tag table
    
    Args:
        df (pd.DataFrame): DataFrame containing tag data
        lead_id_mapping (dict): mapping of staging_lead_id to published_lead_id
        
    Returns:
        int: number of tags inserted
    """
    # Filter rows that have tag data
    tag_df = df[
        (df['tag_list'].notna()) & 
        (df['tag_list'] != 'N/A') & 
        (df['tag_list'] != '') & 
        (df['tag_list'] != None)
    ].copy()
    
    if tag_df.empty:
        print("No tags to insert")
        return 0
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    inserted_count = 0
    
    print(f"🔄 Inserting {len(tag_df)} tags into published.tag...")
    
    for _, row in tag_df.iterrows():
        staging_lead_id = row['staging_lead_id']
        published_lead_id = lead_id_mapping.get(staging_lead_id)
        
        if published_lead_id is None:
            print(f"⚠️ Warning: No published lead_id found for staging lead_id {staging_lead_id}")
            continue
            
        cur.execute("""
            INSERT INTO "published"."tag" (
                "tag_list", 
                "lead_id"
            ) VALUES (%s, %s)
        """, (
            row['tag_list'],
            published_lead_id
        ))
        
        inserted_count += 1
        print(f"✅ Inserted tags: {row['tag_list'][:50]}... (Lead ID: {published_lead_id})")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"🎉 Successfully inserted {inserted_count} tags into published.tag")
    return inserted_count

def process_all_staging_to_published():
    """
    Main function that processes all staging data to published tables
    Calls all individual functions in sequence
    
    Returns:
        dict: summary of all insertions
    """
    print("🚀 Starting complete staging to published transfer...")
    print("="*60)
    
    # Step 1: Get new leads data using your query
    print("Step 1: Getting new leads data...")
    df = get_new_leads_data()
    
    if df.empty:
        print("✅ No new data found - all staging data already exists in published tables")
        return {
            'leads_inserted': 0,
            'clients_inserted': 0,
            'tags_inserted': 0,
            'message': 'No new data to process'
        }
    
    print(f"Found {len(df)} new records to process")
    print("-"*60)
    
    # Step 2: Insert leads first (must be first to get lead_id mapping)
    print("Step 2: Inserting leads...")
    lead_id_mapping = insert_leads_to_published(df)
    print("-"*60)
    
    # Step 3: Insert clients using lead_id mapping
    print("Step 3: Inserting clients...")
    clients_inserted = insert_clients_to_published(df, lead_id_mapping)
    print("-"*60)
    
    # Step 4: Insert tags using lead_id mapping  
    print("Step 4: Inserting tags...")
    tags_inserted = insert_tags_to_published(df, lead_id_mapping)
    print("-"*60)
    
    summary = {
        'leads_inserted': len(lead_id_mapping),
        'clients_inserted': clients_inserted,
        'tags_inserted': tags_inserted,
        'lead_id_mapping': lead_id_mapping,
        'message': 'Success'
    }
    
    print("🎉 COMPLETE! Summary:")
    print(f"   • Leads inserted: {summary['leads_inserted']}")
    print(f"   • Clients inserted: {summary['clients_inserted']}")
    print(f"   • Tags inserted: {summary['tags_inserted']}")
    
    return summary

def check_staging_vs_published():
    """
    Quick check to see how many records are in staging vs published
    and how many are new (would be inserted)
    """
    conn = get_db_connection()
    
    # Count staging records
    staging_count = pd.read_sql_query(
        'SELECT COUNT(*) as count FROM "staging"."lead"', 
        conn
    )['count'][0]
    
    # Count published records  
    published_count = pd.read_sql_query(
        'SELECT COUNT(*) as count FROM "published"."lead"',
        conn
    )['count'][0]
    
    # Count new records (using your duplicate-check query)
    new_records_query = """
    SELECT COUNT(*) as count
    FROM "staging"."lead" SL 
    LEFT JOIN "published"."lead" DL 
    ON SL."link" = DL."link"
    WHERE DL."link" IS NULL
    """
    new_count = pd.read_sql_query(new_records_query, conn)['count'][0]
    
    conn.close()
    
    print(f"📊 DATABASE STATUS:")
    print(f"   • Records in staging.lead: {staging_count}")
    print(f"   • Records in published.lead: {published_count}")
    print(f"   • New records to be inserted: {new_count}")
    print(f"   • Duplicate records (will be skipped): {staging_count - new_count}")
    
    return {
        'staging_count': staging_count,
        'published_count': published_count, 
        'new_count': new_count,
        'duplicate_count': staging_count - new_count
    }





def mark_current_practice_processed():
    """Set status = 1 on the oldest practice with status = 0."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        UPDATE "published"."practice"
        SET "status" = 1
        WHERE "id" = (
            SELECT "id"
            FROM "published"."practice"
            WHERE "status" = 0
            ORDER BY "id" ASC
            LIMIT 1
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()
    print("🔄 Marked oldest unpublished practice (status=0) as processed (status=1).")

def reset_practice_status_if_none_active():
    """
    If there's no row with status = 0, reset all !=0 to 0.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        UPDATE "published"."practice"
        SET "status" = 0
        WHERE NOT EXISTS (
            SELECT 1 FROM "published"."practice" WHERE "status" = 0
        )
        AND "status" != 0
    ''')
    rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    if rows:
        print(f"🔄 Reset {rows} practice rows to status = 0 (none were active).")
    else:
        print("ℹ No need to reset: there is already an active practice row.")
