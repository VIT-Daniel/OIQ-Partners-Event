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
✅ Normalizes dates for MySQL (fixes ISO timezone dates)
✅ ONLY keeps upcoming events (today + future)
✅ Supports NVIDIA date format like 01/11/26 (MM/DD/YY)
"""

import os
import mysql.connector
import re
import requests
import pandas as pd
from datetime import datetime, timezone, date
from html import unescape

# ---------------------------
# ENV LOADING (Railway-safe)
# ---------------------------
if os.getenv("APP_ENV") != "prod":
    from dotenv import load_dotenv
    load_dotenv(".env.dev", override=True)  # dev values win locally

# ---------------------------
# URL Helper
# ---------------------------
def has_valid_url(url: str) -> bool:
    """Only accept real http/https URLs."""
    if not url:
        return False
    url = str(url).strip()
    return url.startswith("http://") or url.startswith("https://")

# ---------------------------
# Datetime Helpers
# ---------------------------
def parse_datetime(value):
    """
    Converts common date formats (including ISO8601 with timezone offsets)
    into a Python datetime (naive UTC).
    Returns None if empty/unparseable.
    """
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    # Normalize Zulu
    s = s.replace("Z", "+00:00")

    # Try ISO8601 (handles offsets like +02:00)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        pass

    # Date-only like 2025-12-17
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return None

    # Fallback formats (✅ includes NVIDIA style like 01/11/26)
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m/%d/%y",   # ✅ NVIDIA: 01/11/26
        "%m/%d/%Y",   # ✅ sometimes full year
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    return None


def is_upcoming(dt: datetime) -> bool:
    """Keep only today + future dates. If dt is None -> skip."""
    if dt is None:
        return False
    return dt.date() >= date.today()


def to_mysql_datetime(value):
    """Convert to MySQL 'YYYY-MM-DD HH:MM:SS' string."""
    dt = parse_datetime(value)
    if not dt:
        return None
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------
# Database Configuration (UNCHANGED)
# ---------------------------
def get_db_connection():
    host = os.getenv("MYSQLHOST")
    user = os.getenv("MYSQLUSER")
    password = os.getenv("MYSQLPASSWORD")  # may be empty string
    database = os.getenv("MYSQL_DATABASE") or os.getenv("MYSQLDATABASE")
    port = os.getenv("MYSQLPORT")

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
        password=password or "",
        database=database,
        port=int(port),
    )

# ---------------------------
# Helper: Clean HTML
# ---------------------------
def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean_text = re.sub(r"<[^>]+>", "", raw_html)
    clean_text = unescape(clean_text)
    clean_text = re.sub(r"\s+", " ", clean_text).strip()
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
# UiPath Parser (valid register_link + upcoming only)
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

    skipped_old = 0

    for item in data:
        category = item.get("category") or "Webinar"

        slug = item.get("slug")
        if slug:
            if str(slug).startswith("http"):
                register_link = slug
            else:
                register_link = f"https://www.uipath.com{slug}"
        else:
            register_link = None

        if not has_valid_url(register_link):
            continue

        start_raw = item.get("date")
        start_dt = parse_datetime(start_raw)

        if not is_upcoming(start_dt):
            skipped_old += 1
            continue

        webinars.append({
            "source": "UiPath",
            "title": item.get("title"),
            "description": clean_html(item.get("teaserBody") or item.get("body")),
            "start_date": start_raw,
            "end_date": None,
            "location": None,
            "register_link": register_link,
            "category": category
        })

    print(f"✅ Found {len(webinars)} UiPath upcoming events (skipped {skipped_old} old).")
    return webinars

# ---------------------------
# NVIDIA Parser (valid register_link + upcoming only)
# ---------------------------
def parse_nvidia(json_data: dict) -> list:
    events = []
    data = json_data.get("events", []) if isinstance(json_data, dict) else json_data

    if not data:
        print("⚠️ No NVIDIA 'events' found.")
        return []

    skipped_old = 0

    for item in data:
        category = item.get("type") or "Conference"
        register_link = item.get("url")

        if not has_valid_url(register_link):
            continue

        start_raw = item.get("startDate")
        start_dt = parse_datetime(start_raw)

        if not is_upcoming(start_dt):
            skipped_old += 1
            continue

        events.append({
            "source": "NVIDIA",
            "title": item.get("title"),
            "description": None,
            "start_date": start_raw,
            "end_date": item.get("endDate"),
            "location": item.get("location") or item.get("venue"),
            "register_link": register_link,
            "category": category
        })

    print(f"✅ Found {len(events)} NVIDIA upcoming events (skipped {skipped_old} old).")
    return events

# ---------------------------
# AWS Parser (valid register_link + upcoming only)
# ---------------------------
def parse_aws(json_data: dict) -> list:
    events = []
    items = json_data.get("items", [])

    if not items:
        print("⚠️ No AWS 'items' found.")
        return []

    skipped_no_date = 0
    skipped_old = 0

    for i in items:
        item = i.get("item", {})
        fields = item.get("additionalFields", {})

        category = fields.get("eventType") or "Webinar"
        register_link = fields.get("ctaLink") or fields.get("primaryCTALink")

        if not has_valid_url(register_link):
            continue

        possible_dates = [
            fields.get("startDate"),
            fields.get("eventDate"),
            fields.get("date"),
            item.get("dateCreated"),  # fallback
        ]

        start_raw = None
        start_dt = None
        for d in possible_dates:
            dt = parse_datetime(d)
            if dt:
                start_raw = d
                start_dt = dt
                break

        if not start_dt:
            skipped_no_date += 1
            continue

        if not is_upcoming(start_dt):
            skipped_old += 1
            continue

        events.append({
            "source": "AWS",
            "title": fields.get("title"),
            "description": clean_html(fields.get("bodyBack") or fields.get("body")),
            "start_date": start_raw,
            "end_date": None,
            "location": None,
            "register_link": register_link,
            "category": category
        })

    print(f"✅ Found {len(events)} AWS upcoming events (skipped {skipped_old} old, {skipped_no_date} missing-date).")
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
def save_to_db(events: list) -> int:
    if not events:
        print("⚠️ No data to save to DB")
        return 0

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM partner_events")
    before_count = cursor.fetchone()[0]

    insert_query = """
    INSERT INTO partner_events
    (source, category, title, description, start_date, end_date, location, register_link)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      category = VALUES(category),
      title = VALUES(title),
      description = VALUES(description),
      start_date = VALUES(start_date),
      end_date = VALUES(end_date),
      location = VALUES(location),
      register_link = VALUES(register_link),
      updated_at = CURRENT_TIMESTAMP
    """

    for e in events:
        cursor.execute(insert_query, (
            e.get("source"),
            e.get("category"),
            e.get("title"),
            e.get("description"),
            to_mysql_datetime(e.get("start_date")),
            to_mysql_datetime(e.get("end_date")),
            e.get("location"),
            e.get("register_link"),
        ))

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM partner_events")
    after_count = cursor.fetchone()[0]

    inserted_count = after_count - before_count

    cursor.close()
    conn.close()

    print(f"🗄️ {inserted_count} new rows inserted (existing rows may have been updated).")
    return inserted_count

# ---------------------------
# Logging
# ---------------------------
def write_log(log_data: dict):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = [
        f"\n===== Run Log - {timestamp} =====",
        f"AWS Events (upcoming): {log_data.get('aws', 0)}",
        f"NVIDIA Events (upcoming): {log_data.get('nvidia', 0)}",
        f"UiPath Events (upcoming): {log_data.get('uipath', 0)}",
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
    # uipath_url = "https://www.uipath.com/steam-resources/page-data/resources/automation-webinars/page-data.json"
    # nvidia_url = "https://www.nvidia.com/content/dam/en-zz/Solutions/about-nvidia/calendar/en-us.json"
    # aws_url = "https://aws.amazon.com/api/dirs/items/search?item.directoryId=alias%23events-webinars-interactive-cards&item.locale=en_US&tags.id=%21GLOBAL%23local-tags-events-master-series%23third-party&tags.id=%21GLOBAL%23local-tags-series%23third-party&tags.id=%21GLOBAL%23local-tags-flag%23archived&sort_by=item.dateCreated&sort_order=desc&size=8"

    uipath_url = "https://www.uipath.com/steam-resources/page-data/events/page-data.json"
    nvidia_url = "https://www.nvidia.com/content/dam/en-zz/Solutions/about-nvidia/calendar/en-us.json?t=1766414224225"
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

    inserted_total = save_to_db(all_events)
    log_counts["inserted_total"] = inserted_total

    write_log(log_counts)

if __name__ == "__main__":
    main()
