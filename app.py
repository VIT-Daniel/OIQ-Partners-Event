from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import math
import mysql.connector
import os

app = Flask(__name__)
CORS(app)

# ---------------------------
# Database Connection
# ---------------------------
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQLHOST", "localhost"),
        user=os.getenv("MYSQLUSER", "root"),
        password=os.getenv("MYSQLPASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", ""),
        port=int(os.getenv("MYSQLPORT", 3306))  # ✅ Added explicit port support
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
    cursor.execute(
        f"SELECT * FROM partner_events ORDER BY start_date DESC LIMIT {per_page} OFFSET {offset}"
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

    count_query = f"SELECT COUNT(*) AS total FROM partner_events {where_clause}"
    cursor.execute(count_query, tuple(params))
    total = cursor.fetchone()['total']
    total_pages = math.ceil(total / per_page)

    query = f"""
        SELECT * FROM partner_events
        {where_clause}
        ORDER BY start_date DESC
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, tuple(params + [per_page, offset]))
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
# Run the App (✅ For Railway)
# ---------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # Railway assigns a port dynamically
    app.run(host="0.0.0.0", port=port)
