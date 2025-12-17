from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import math
import mysql.connector
import os
from dotenv import load_dotenv

if os.getenv("APP_ENV") != "prod":
    load_dotenv(".env.dev")





# ---------------------------
# Load .env file (local dev)
# ---------------------------
load_dotenv()

print("APP_ENV =", os.getenv("APP_ENV"))
print("MYSQLHOST =", os.getenv("MYSQLHOST"))
print("MYSQLPORT =", os.getenv("MYSQLPORT"))


app = Flask(__name__)

# ---------------------------
# Secret Key (required for sessions/security)
# ---------------------------
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-fallback-key")

CORS(app)

# ---------------------------
# Database Connection
# ---------------------------
from urllib.parse import urlparse

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
# Main Web Route
# ---------------------------
@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    per_page = 16
    page = request.args.get('page', 1, type=int)

    cursor.execute("SELECT COUNT(*) AS total FROM partner_events")
    total = cursor.fetchone()['total']
    total_pages = math.ceil(total / per_page)

    offset = (page - 1) * per_page

    # ✅ safer than f-string in SQL
    cursor.execute(
        """
        SELECT * FROM partner_events
        ORDER BY start_date DESC
        LIMIT %s OFFSET %s
        """,
        (per_page, offset)
    )

    events = cursor.fetchall()
    conn.close()

    window_size = 10
    start_page = ((page - 1) // window_size) * window_size + 1
    end_page = min(start_page + window_size - 1, total_pages)

    return render_template(
        'events.html',
        events=events,
        page=page,
        total_pages=total_pages,
        start_page=start_page,
        end_page=end_page
    )

# ---------------------------
# API Endpoint (Filters + Pagination)
# ---------------------------
@app.route('/api/events', methods=['GET'])
def get_events():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    per_page = 16
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * per_page

    q = request.args.get('q', '', type=str).strip()
    source = request.args.get('source', '', type=str).strip()
    category = request.args.get('category', '', type=str).strip()

    filters = []
    params = []

    if q:
        filters.append("(title LIKE %s OR description LIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])
    if source:
        filters.append("source = %s")
        params.append(source)
    if category:
        filters.append("category = %s")
        params.append(category)

    where_clause = "WHERE " + " AND ".join(filters) if filters else ""

    cursor.execute(
        f"SELECT COUNT(*) AS total FROM partner_events {where_clause}",
        tuple(params)
    )
    total = cursor.fetchone()['total']
    total_pages = math.ceil(total / per_page)

    cursor.execute(
        f"""
        SELECT * FROM partner_events
        {where_clause}
        ORDER BY start_date DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params + [per_page, offset])
    )

    events = cursor.fetchall()
    conn.close()

    window_size = 10
    start_page = ((page - 1) // window_size) * window_size + 1
    end_page = min(start_page + window_size - 1, total_pages)

    return jsonify({
        "page": page,
        "total_pages": total_pages,
        "total_events": total,
        "start_page": start_page,
        "end_page": end_page,
        "filters": {
            "q": q,
            "source": source,
            "category": category
        },
        "events": events
    })

# ---------------------------
# Run the App (For Railway)
# ---------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
