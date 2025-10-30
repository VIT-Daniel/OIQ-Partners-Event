"""
Multi-Source Event Scraper
------------------------------------------------
Sources:
1. UiPath Webinars API
2. NVIDIA Events API
3. AWS Events API (JSON)

Features:
✅ Duplicate filtering
✅ Logging system
✅ Source identifiers
✅ HTML-cleaned descriptions
✅ Unified CSV export
✅ Optional DB support (commented)

Final script
Last updated: 2025-10-29
Last updated: 2025-10-30

"""
import mysql.connector
from mysql.connector import Error
import json
import re
import requests
import pandas as pd
from datetime import datetime
from html import unescape


# ---------------------------
# Database Configuration
# ---------------------------
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",        # or your DB host
        user="root",             # your MySQL username
        password="", # your MySQL password
        database="partner_events" # the DB name you created
    )




# ---------------------------
# Helper: Clean HTML
# ---------------------------
def clean_html(raw_html: str) -> str:
    """Remove HTML tags and unescape HTML entities from text."""
    if not raw_html:
        return ""
    clean_text = re.sub(r"<[^>]+>", "", raw_html)  # remove HTML tags
    clean_text = unescape(clean_text)  # convert &nbsp;, &amp;, etc.
    clean_text = re.sub(r"\s+", " ", clean_text).strip()  # normalize spaces
    return clean_text


# ---------------------------
# Generic JSON fetcher
# ---------------------------
def fetch_json(url: str) -> dict:
    print(f"\n🌐 Fetching data from: {url}")
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return response.json()


# ---------------------------
# UiPath Parser
# ---------------------------
def parse_uipath(json_data: dict) -> list:
    webinars = []

    def find_resource_data(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "resourceData" and isinstance(v, list):
                    return v
                found = find_resource_data(v)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = find_resource_data(item)
                if found:
                    return found
        return None

    data = find_resource_data(json_data)
    if not data:
        print("⚠️ No UiPath 'resourceData' found.")
        return []

    for item in data:
        # Try to get a category; default to 'Webinar'
        category = item.get("category") or "Webinar"

        # --- Fix malformed register_link ---
        slug = item.get("slug")
        if slug:
            if slug.startswith("http"):
                register_link = slug
            else:
                register_link = f"https://www.uipath.com{slug}"
        else:
            register_link = None

        webinars.append({
            "source": "UiPath",
            "title": item.get("title"),
            "description": clean_html(item.get("teaserBody") or item.get("body")),
            "start_date": item.get("date"),
            "end_date": None,
            "location": None,
            "register_link": register_link,
            "category": category
        })

    print(f"✅ Found {len(webinars)} UiPath events.")
    return webinars



# ---------------------------
# NVIDIA Parser
# ---------------------------
def parse_nvidia(json_data: dict) -> list:
    events = []

    data = json_data.get("events", []) if isinstance(json_data, dict) else json_data
    if not data:
        print("⚠️ No NVIDIA 'events' found.")
        return []

    for item in data:
        # Use 'type' as category; default to 'Conference'
        category = item.get("type") or "Conference"

        events.append({
            "source": "NVIDIA",
            "title": item.get("title"),
            "description": None,
            "start_date": item.get("startDate"),
            "end_date": item.get("endDate"),
            "location": item.get("location") or item.get("venue"),
            "register_link": item.get("url"),
            "category": category
        })

    print(f"✅ Found {len(events)} NVIDIA events.")
    return events


# ---------------------------
# AWS Parser
# ---------------------------
def parse_aws(json_data: dict) -> list:
    events = []
    items = json_data.get("items", [])
    if not items:
        print("⚠️ No AWS 'items' found.")
        return []

    for i in items:
        item = i.get("item", {})
        fields = item.get("additionalFields", {})

        # Default category to 'Webinar' if not specified
        category = fields.get("eventType") or "Webinar"

        events.append({
            "source": "AWS",
            "title": fields.get("title"),
            "description": clean_html(fields.get("bodyBack") or fields.get("body")),
            "start_date": item.get("dateCreated"),
            "end_date": item.get("dateUpdated"),
            "location": None,
            "register_link": fields.get("ctaLink") or fields.get("primaryCTALink"),
            "category": category
        })

    print(f"✅ Found {len(events)} AWS events.")
    return events



# ---------------------------
# Remove Duplicates
# ---------------------------
def remove_duplicates(events: list) -> list:
    df = pd.DataFrame(events)
    before = len(df)
    df.drop_duplicates(subset=["title", "start_date", "source"], inplace=True)
    after = len(df)
    removed = before - after
    if removed > 0:
        print(f"🧹 Removed {removed} duplicate records.")
    return df.to_dict(orient="records")


# ---------------------------
# Save to CSV
# ---------------------------
def save_to_csv(events: list, base_filename: str):
    if not events:
        print(f"⚠️ No data to save for {base_filename}")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    filename = f"{base_filename}_{timestamp}.csv"

    df = pd.DataFrame(events)
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"💾 Saved {len(df)} records → {filename}")


#-----------------
# Db 
#----------------

def save_to_db(events: list):
    if not events:
        print("⚠️ No data to save to DB")
        return 0  # return 0 inserted

    conn = get_db_connection()
    cursor = conn.cursor()

    # Count before inserting
    cursor.execute("SELECT COUNT(*) FROM partner_events")
    before_count = cursor.fetchone()[0]

    insert_query = """
    INSERT IGNORE INTO partner_events 
    (source, category, title, description, start_date, end_date, location, register_link)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    for e in events:
        cursor.execute(insert_query, (
            e.get("source"),
            e.get("category"),
            e.get("title"),
            e.get("description"),
            e.get("start_date"),
            e.get("end_date"),
            e.get("location"),
            e.get("register_link"),
        ))

    conn.commit()

    # Count after inserting
    cursor.execute("SELECT COUNT(*) FROM partner_events")
    after_count = cursor.fetchone()[0]

    inserted_count = after_count - before_count

    cursor.close()
    conn.close()

    print(f"🗄️ {inserted_count} new events inserted into DB (duplicates ignored).")
    return inserted_count

    
    
# ---------------------------
# Logging
# ---------------------------
def write_log(log_data: dict):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = [
        f"\n===== Run Log - {timestamp} =====",
        f"AWS Events: {log_data.get('aws', 0)}",
        f"NVIDIA Events: {log_data.get('nvidia', 0)}",
        f"UiPath Events: {log_data.get('uipath', 0)}",
        f"Total (after duplicates): {log_data.get('total', 0)}",
        f"🆕 Newly inserted to DB: {log_data.get('inserted_total', 0)}",
        "==============================="
    ]
    with open("scraper_log.txt", "a", encoding="utf-8") as log_file:
        log_file.write("\n".join(log_entry) + "\n")
    print("🪵 Log updated → scraper_log.txt")
    
    

# ---------------------------
# MAIN SCRIPT
# ---------------------------
def main():
    # Define URLs
    uipath_url = "https://www.uipath.com/steam-resources/page-data/resources/automation-webinars/page-data.json"
    nvidia_url = "https://www.nvidia.com/content/dam/en-zz/Solutions/about-nvidia/calendar/en-us.json"
    aws_url = "https://aws.amazon.com/api/dirs/items/search?item.directoryId=alias%23events-webinars-interactive-cards&item.locale=en_US&tags.id=%21GLOBAL%23local-tags-events-master-series%23third-party&tags.id=%21GLOBAL%23local-tags-series%23third-party&tags.id=%21GLOBAL%23local-tags-flag%23archived&sort_by=item.dateCreated&sort_order=desc&size=8"

    all_events = []
    log_counts = {}

    # --- UiPath ---
    uipath_json = fetch_json(uipath_url)
    uipath_events = parse_uipath(uipath_json)
    log_counts["uipath"] = len(uipath_events)
    all_events.extend(uipath_events)

    # --- NVIDIA ---
    nvidia_json = fetch_json(nvidia_url)
    nvidia_events = parse_nvidia(nvidia_json)
    log_counts["nvidia"] = len(nvidia_events)
    all_events.extend(nvidia_events)

    # --- AWS ---
    aws_json = fetch_json(aws_url)
    aws_events = parse_aws(aws_json)
    log_counts["aws"] = len(aws_events)
    all_events.extend(aws_events)

    # --- Remove duplicates ---
    all_events = remove_duplicates(all_events)
    log_counts["total"] = len(all_events)

    # --- Save ---
    save_to_csv(all_events, "all_partner_events")
    
    # --- Save to database ---
    save_to_db(all_events)
    
    inserted_total = save_to_db(all_events)
    log_counts["inserted_total"] = inserted_total


    # --- Log summary ---
    write_log(log_counts)




if __name__ == "__main__":
    main()
