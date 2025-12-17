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
✅ DB support
✅ ONLY keeps events with valid registration links (http/https)

"""

import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

import json
import re
import requests
import pandas as pd
from datetime import datetime
from html import unescape


# ---------------------------
# ENV LOADING (keep your behavior)
# ---------------------------
if os.getenv("APP_ENV") != "prod":
    load_dotenv(".env.dev", override=True)  # dev values win locally
else:
    load_dotenv()  # in case you ever run prod locally (optional)

# Optional debug (safe)
print("APP_ENV =", os.getenv("APP_ENV"))
print("MYSQLHOST =", os.getenv("MYSQLHOST"))
print("MYSQLPORT =", os.getenv("MYSQLPORT"))


# ---------------------------
# URL Helper (NEW)
# ---------------------------
def has_valid_url(url: str) -> bool:
    """Only accept real http/https URLs."""
    if not url:
        return False
    url = str(url).strip()
    return url.startswith("http://") or url.startswith("https://")


# ---------------------------
# Database Configuration (UNCHANGED)
# ---------------------------
def get_db_connection():
    host = os.getenv("MYSQLHOST")
    user = os.getenv("MYSQLUSER")
    password = os.getenv("MYSQLPASSWORD")  # may be empty string
    database = os.getenv("MYSQL_DATABASE") or os.getenv("MYSQLDATABASE")
    port = os.getenv("MYSQLPORT")

    # Password can be empty, others cannot
    missing = [k for k, v in {
        "MYSQLHOST": host,
        "MYSQLUSER": user,
        "MYSQL_DATABASE or MYSQLDATABASE": database,
        "MYSQLPORT": port,
    }.items() if not v]

    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    return mysql.connector.connect(
        host=host,
        user=user,
        password=password or "",  # empty password allowed
        database=database,
        port=int(port),
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
# UiPath Parser (UPDATED: keep only valid register_link)
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
        category = item.get("category") or "Webinar"

        slug = item.get("slug")
        if slug:
            if slug.startswith("http"):
                register_link = slug
            else:
                register_link = f"https://www.uipath.com{slug}"
        else:
            register_link = None

        # ✅ Filter: must have valid URL
        if not has_valid_url(register_link):
            continue

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

    print(f"✅ Found {len(webinars)} UiPath events (with valid URL).")
    return webinars


# ---------------------------
# NVIDIA Parser (UPDATED: keep only valid register_link)
# ---------------------------
def parse_nvidia(json_data: dict) -> list:
    events = []

    data = json_data.get("events", []) if isinstance(json_data, dict) else json_data
    if not data:
        print("⚠️ No NVIDIA 'events' found.")
        return []

    for item in data:
        category = item.get("type") or "Conference"
        register_link = item.get("url")

        # ✅ Filter: must have valid URL
        if not has_valid_url(register_link):
            continue

        events.append({
            "source": "NVIDIA",
            "title": item.get("title"),
            "description": None,
            "start_date": item.get("startDate"),
            "end_date": item.get("endDate"),
            "location": item.get("location") or item.get("venue"),
            "register_link": register_link,
            "category": category
        })

    print(f"✅ Found {len(events)} NVIDIA events (with valid URL).")
    return events


# ---------------------------
# AWS Parser (UPDATED: keep only valid register_link)
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

        category = fields.get("eventType") or "Webinar"
        register_link = fields.get("ctaLink") or fields.get("primaryCTALink")

        # ✅ Filter: must have valid URL
        if not has_valid_url(register_link):
            continue

        events.append({
            "source": "AWS",
            "title": fields.get("title"),
            "description": clean_html(fields.get("bodyBack") or fields.get("body")),
            "start_date": item.get("dateCreated"),
            "end_date": item.get("dateUpdated"),
            "location": None,
            "register_link": register_link,
            "category": category
        })

    print(f"✅ Found {len(events)} AWS events (with valid URL).")
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


# ---------------------------
# Save to DB
# ---------------------------
def save_to_db(events: list):
    if not events:
        print("⚠️ No data to save to DB")
        return 0

    conn = get_db_connection()
    cursor = conn.cursor()

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
        f"AWS Events (valid URL): {log_data.get('aws', 0)}",
        f"NVIDIA Events (valid URL): {log_data.get('nvidia', 0)}",
        f"UiPath Events (valid URL): {log_data.get('uipath', 0)}",
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
    uipath_url = "https://www.uipath.com/steam-resources/page-data/resources/automation-webinars/page-data.json"
    nvidia_url = "https://www.nvidia.com/content/dam/en-zz/Solutions/about-nvidia/calendar/en-us.json"
    aws_url = "https://aws.amazon.com/api/dirs/items/search?item.directoryId=alias%23events-webinars-interactive-cards&item.locale=en_US&tags.id=%21GLOBAL%23local-tags-events-master-series%23third-party&tags.id=%21GLOBAL%23local-tags-series%23third-party&tags.id=%21GLOBAL%23local-tags-flag%23archived&sort_by=item.dateCreated&sort_order=desc&size=8"

    all_events = []
    log_counts = {}

    uipath_events = parse_uipath(fetch_json(uipath_url))
    log_counts["uipath"] = len(uipath_events)
    all_events.extend(uipath_events)

    nvidia_events = parse_nvidia(fetch_json(nvidia_url))
    log_counts["nvidia"] = len(nvidia_events)
    all_events.extend(nvidia_events)

    aws_events = parse_aws(fetch_json(aws_url))
    log_counts["aws"] = len(aws_events)
    all_events.extend(aws_events)

    all_events = remove_duplicates(all_events)
    log_counts["total"] = len(all_events)

    save_to_csv(all_events, "all_partner_events")

    # ✅ FIX: Save to DB ONCE (you had it twice)
    inserted_total = save_to_db(all_events)
    log_counts["inserted_total"] = inserted_total

    write_log(log_counts)


if __name__ == "__main__":
    main()
