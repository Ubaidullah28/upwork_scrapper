import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import random
import sys
import re
from datetime import datetime, timedelta
import json
import pyautogui
import os
from dotenv import load_dotenv
from database_operation import get_search_queries_from_db, read_last_scrape_time, update_scrape_time
import json

load_dotenv()
sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import json

def json_to_dataframe(json_data):
    """
    Convert JSON data to a clean pandas DataFrame
    
    Args:
        json_data (str or list): JSON string or list of dictionaries
        
    Returns:
        pd.DataFrame: Clean pandas DataFrame with job data
    """
    # If json_data is a string, parse it
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        data = json_data
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # If DataFrame is empty, return empty DataFrame with expected columns
    if df.empty:
        expected_columns = [
            'Search Query', 'Title', 'Job Link', 'Tags', 'Client Spent',
            'Payment Info', 'Budget Type', 'Lower Hourly Rate', 
            'Higher Hourly Rate', 'Fixed Price', 'Payment Verified/Unverified',
            'Description', 'Posted Time'
        ]
        return pd.DataFrame(columns=expected_columns)
    
    # Clean the data
    df = df.fillna('N/A')  # Fill NaN values with 'N/A'
    
    # Convert Posted Time to datetime if it's not already
    if 'Posted Time' in df.columns:
        df['Posted Time'] = pd.to_datetime(df['Posted Time'], errors='coerce')
    
    # Remove any duplicate rows based on Job Link
    if 'Job Link' in df.columns:
        df = df.drop_duplicates(subset=['Job Link'], keep='first')
    
    # Reset index
    df = df.reset_index(drop=True)
    
    return df

def human_sleep(min_s=2, max_s=5):
    time.sleep(random.uniform(min_s, max_s))

def slow_scroll(driver, pause_time=1):
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        human_sleep(pause_time, pause_time + 2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def simulate_typing(element, text):
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.05, 0.15))

def parse_posted_time(text):
    now = datetime.now()
    text = text.lower()
    if 'minute' in text:
        return now - timedelta(minutes=int(re.search(r'\d+', text).group()))
    elif 'hour' in text:
        return now - timedelta(hours=int(re.search(r'\d+', text).group()))
    elif 'day' in text:
        return now - timedelta(days=int(re.search(r'\d+', text).group()))
    return now

def click_cloudflare_checkbox_pyautogui():
    time.sleep(5.5)
    pyautogui.click(799, 495)
    time.sleep(5)

def wait_until_found_and_click(image_name, confidence=0.9):
    time.sleep(8)
    count = 0
    while True:
        count += 1
        try:
            location = pyautogui.locateOnScreen(image_name, confidence=confidence)
            if location:
                pyautogui.moveTo(pyautogui.center(location), duration=0.5)
                pyautogui.click()
                return
        except Exception as e:
            if count > 5:
                break
        time.sleep(2)

def login_with_google(driver, email, password):
    driver.get("https://www.upwork.com/ab/account-security/login")
    human_sleep(3, 5)
    try:
        google_btn = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#login_google_submit > span"))
        )
        google_btn.click()
    except:
        return
    human_sleep(4, 6)
    driver.switch_to.window(driver.window_handles[-1])
    try:
        email_input = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="email"]'))
        )
        simulate_typing(email_input, email)
        email_input.send_keys(Keys.ENTER)
    except:
        return
    human_sleep(4, 6)
    try:
        password_input = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="password"]'))
        )
        simulate_typing(password_input, password)
        password_input.send_keys(Keys.ENTER)
    except:
        return
    human_sleep(8, 12)
    driver.switch_to.window(driver.window_handles[0])

def extract_jobs_from_current_page(driver, last_scrape_time, search_query):
    jobs_data = []
    print("🔍 Looking for job section...")
    
    try:
        job_section = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "section > article"))
        ).find_element(By.XPATH, "./..")
        print("✅ Found job section!")
    except:
        print("❌ Could not find job section - page might not have loaded properly")
        return jobs_data
    
    human_sleep(2, 4)
    job_cards = job_section.find_elements(By.XPATH, './article')
    print(f"📋 Found {len(job_cards)} job cards on this page")
    
    if len(job_cards) == 0:
        print("❌ No job cards found - this is the problem!")
        return jobs_data
    
    for i, job in enumerate(job_cards):
        print(f"📝 Processing job {i+1}/{len(job_cards)}...")
        
        try:
            title_elem = job.find_element(By.CSS_SELECTOR, 'h2.job-tile-title a')
            title = title_elem.text.strip()
            job_link = title_elem.get_attribute("href")
            print(f"   Title: {title[:50]}...")
        except:
            title, job_link = "N/A", "N/A"
            print("   ⚠️ Could not extract title")
            
        try:
            tags_elements = job.find_elements(By.CSS_SELECTOR, 'div.air3-token-container button')
            tags = ', '.join([tag.text.strip() for tag in tags_elements if tag.text.strip() != ''])
        except:
            tags = "N/A"
            
        try:
            spent = job.find_element(By.CSS_SELECTOR, 'ul.d-flex.align-items-center.flex-wrap.text-light.gap-wide.text-base-sm.mb-4 li:nth-child(3) > div').text.strip()
        except:
            spent = "N/A"
            
        try:
            payment = job.find_element(By.CSS_SELECTOR, 'ul.job-tile-info-list.text-base-sm.mb-4 li:nth-child(3)').text.strip()
        except:
            payment = "N/A"
            
        try:
            payment_verified = job.find_element(By.CSS_SELECTOR, 'ul.d-flex.align-items-center.flex-wrap.text-light.gap-wide.text-base-sm.mb-4 li:nth-child(1) > div').text.strip()
        except:
            payment_verified = "N/A"
            
        try:
            description = job.find_element(By.CSS_SELECTOR, 'p.mb-0.text-body-sm').text.strip()
        except:
            description = "N/A"
            
        try:
            posted_text = job.find_element(By.CSS_SELECTOR, 'div.job-tile-header small > span:nth-child(2)').text.strip()
            posted_time = parse_posted_time(posted_text)
        except:
            posted_text = "N/A"
            posted_time = datetime.now()

        # Extract Budget Type from two different selectors
        budget_parts = []
        try:
            budget_part1 = job.find_element(By.CSS_SELECTOR, 'ul.job-tile-info-list.text-base-sm.mb-4 li:nth-child(1) > strong').text.strip()
            if budget_part1:
                budget_parts.append(budget_part1)
        except:
            pass
        
        try:
            budget_part2 = job.find_element(By.CSS_SELECTOR, 'ul.job-tile-info-list.text-base-sm.mb-4 li:nth-child(2) > strong').text.strip()
            if budget_part2:
                budget_parts.append(budget_part2)
        except:
            pass
        
        budget_raw = ' '.join(budget_parts) if budget_parts else "N/A"
        
        # Parse Budget Type and extract hourly rates
        lower_hourly_rate = "N/A"
        higher_hourly_rate = "N/A"
        fixed_price = "N/A"
        budget_type = budget_raw
        
        if budget_raw != "N/A":
            rate_pattern = r'\$(\d+\.?\d*)\s*-\s*\$(\d+\.?\d*)'
            rate_match = re.search(rate_pattern, budget_raw)
            
            if rate_match:
                lower_hourly_rate = f"${rate_match.group(1)}"
                higher_hourly_rate = f"${rate_match.group(2)}"
                budget_type = re.sub(rate_pattern, '', budget_raw).strip()
                budget_type = ' '.join(budget_type.split())
        
        if "Fixed price" in budget_type and payment != "N/A":
            price_pattern = r'\$(\d+(?:\.\d{2})?)'
            price_match = re.search(price_pattern, payment)
            if price_match:
                fixed_price = f"${price_match.group(1)}"

        # 🚫 DISABLED TIME FILTERING FOR TESTING - This was probably blocking all jobs!
        # if last_scrape_time and posted_time <= last_scrape_time:
        #     print(f"   ⏰ Skipping job due to time filter")
        #     continue

        jobs_data.append({
            'Search Query': search_query,
            'Title': title,
            'Job Link': job_link,
            'Tags': tags,
            'Client Spent': spent,
            'Payment Info': payment,
            'Budget Type': budget_type,
            'Lower Hourly Rate': lower_hourly_rate,
            'Higher Hourly Rate': higher_hourly_rate,
            'Fixed Price': fixed_price,
            'Payment Verified/Unverified': payment_verified,
            'Description': description,
            'Posted Time': posted_time.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        print(f" Added job successfully!")
        human_sleep(1, 2)
    
    print(f"Collected {len(jobs_data)} jobs from this page")
    return jobs_data

def navigate_to_page(driver, page_number, last_scrape_time, all_jobs, search_query):
    try:
        # Wait for any pagination button to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, f"button[data-ev-page_index='{page_number}']"))
        )

        # Wait for page button to be clickable
        page_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f"button[data-ev-page_index='{page_number}']"))
        )

        page_button.click()
        human_sleep(5, 8)
        slow_scroll(driver)
        return all_jobs + extract_jobs_from_current_page(driver, last_scrape_time, search_query)
    except Exception as e:
        return all_jobs

def scrape_upwork_jobs():
    all_jobs = []
    search_queries = get_search_queries_from_db()
    
    if not search_queries:
        return json.dumps([], ensure_ascii=False)

    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = uc.Chrome(options=options)

    try:
        login_with_google(driver, os.getenv('GMAIL_EMAIL'), os.getenv('GMAIL_PASSWORD'))
        human_sleep(7, 10)

        query_text = search_queries["search_criteria"]
        search_url = f"https://www.upwork.com/nx/jobs/search/?q={query_text.replace(' ', '%20')}&sort=recency"

        driver.get(search_url)
        human_sleep(3, 5)
        wait_until_found_and_click(os.getenv('CLOUDFLARE_IMAGE_PATH'))

        last_scrape_time = read_last_scrape_time(query_text)
        query_jobs = extract_jobs_from_current_page(driver, last_scrape_time, query_text)

        for page_num in []:
            query_jobs = navigate_to_page(driver, page_num, last_scrape_time, query_jobs, query_text)

        all_jobs.extend(query_jobs)
        update_scrape_time(query_text)

        # Since timestamps are already converted to strings, create DataFrame directly
        df = pd.DataFrame(all_jobs)

        if not df.empty:
            df.to_csv("upwork_jobs.csv", index=False, encoding='utf-8-sig')

        # Return JSON directly from the list of dictionaries (no DataFrame conversion needed)
        return json.dumps(all_jobs, ensure_ascii=False, default=str)

    finally:
        try:
            driver.quit()
        except Exception:
            pass


