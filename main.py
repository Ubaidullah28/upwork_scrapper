import sys
import json
from upwork_scraping import scrape_upwork_jobs, json_to_dataframe
from database_operation import (
    get_search_queries_from_db,
    get_source_info,
    insert_jobs_into_public_job,
    read_last_scrape_time,
    insert_raw_json_data,
    insert_df_into_staging_lead,
    get_max_lead_id,
    insert_df_into_staging_client,
    insert_df_into_staging_tag,
    process_all_staging_to_published,
    mark_current_practice_processed,
    reset_practice_status_if_none_active

)

from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

def main():
    try:
        # Check if there are any search queries to process
        search_queries = get_search_queries_from_db()
        
        if not search_queries:
            print("No search queries found with status 0")
            return
        
        print(f"Processing search query: {search_queries['search_criteria']}")
        
        # CHECK THE TIME FILTER - This might be the problem!
        last_time = read_last_scrape_time(search_queries["search_criteria"])
        print(f"Last scrape time: {last_time}")
        print(f"Current time: {datetime.now()}")
        
        # Execute the scraping process
        print("Starting scraping...")
        json_result = scrape_upwork_jobs()
        
        # Check if we got data
        print(f"JSON result length: {len(json_result)}")
        
        # Convert JSON to clean DataFrame
        df = json_to_dataframe(json_result)
        print(f"DataFrame created with {len(df)} rows")

        # Get source_id and source_name
        source_info = get_source_info()

        # Insert jobs into public.job
        insert_jobs_into_public_job(df, source_info, search_queries["search_criteria"])
        print("data inserted into public.job")

         # Insert raw JSON data using the latest job ID from public.job
        job_id = insert_raw_json_data(json_result)
        
        if job_id:
            print(f"Raw JSON data inserted successfully with job_id: {job_id}")
        else:
            print("Failed to insert raw JSON data")
        
        # Get max lead_id before lead insertion
        max_lead_id_before = get_max_lead_id()
        print(f"Max lead_id before lead insertion: {max_lead_id_before}")

         # Insert DataFrame into staging.lead using latest raw_id
        raw_id = insert_df_into_staging_lead(df)
        
        if raw_id:
            print(f"DataFrame data inserted into staging.lead successfully with raw_id: {raw_id}")
        else:
            print("Failed to insert DataFrame into staging.lead")

        # Insert DataFrame into staging.client using new leads only
        client_count = insert_df_into_staging_client(df, max_lead_id_before)
        
        if client_count > 0:
            print(f"Client data inserted into staging.client successfully. {client_count} records added.")
        else:
            print("No client data inserted - no new leads found")

        # Insert DataFrame into staging.tag using new leads only
        tag_count = insert_df_into_staging_tag(df, max_lead_id_before)

        if tag_count > 0:
            print(f"Tag data inserted into staging.tag successfully. {tag_count} records added.")
        else:
            print("No tag data inserted - no new leads found")

        result = process_all_staging_to_published()
        print("data is inserted into published")     


        mark_current_practice_processed()
    
        reset_practice_status_if_none_active()


        # Display DataFrame information
        print(f"Scraped {len(df)} jobs successfully!")
        print(f"\nDataFrame Shape: {df.shape}")
        print("\nColumn Names:")
        for i, col in enumerate(df.columns, 1):
            print(f"{i}. {col}")
        
        if len(df) > 0:
            print("\nFirst 3 rows:")
            print(df.head(3).to_string())
        else:
            print("\n DataFrame is EMPTY! This is the problem.")
            print("The time filter might be blocking all jobs.")
            print("Or Cloudflare might be blocking the scraper.")
        
        return df
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    main()