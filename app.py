from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import math
import mysql.connector

app = Flask(__name__)
CORS(app)

# ---------------------------
# Database Connection
# ---------------------------
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="partner_events"
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

    # Count total rows
    cursor.execute("SELECT COUNT(*) AS total FROM partner_events")
    total = cursor.fetchone()['total']
    total_pages = math.ceil(total / per_page)

    # Fetch paginated events
    offset = (page - 1) * per_page
    cursor.execute(
        f"SELECT * FROM partner_events ORDER BY start_date DESC LIMIT {per_page} OFFSET {offset}"
    )
    events = cursor.fetchall()
    conn.close()

    # ✅ Improved pagination window logic (1–10, 11–20, etc.)
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
# ✅ Enhanced API Endpoint with Filters + Pagination
# ---------------------------
@app.route('/api/events', methods=['GET'])
def get_events():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Pagination
    per_page = 16
    page = request.args.get('page', 1, type=int)
    offset = (page - 1) * per_page

    # Filters
    q = request.args.get('q', '', type=str).strip()
    source = request.args.get('source', '', type=str).strip()
    category = request.args.get('category', '', type=str).strip()

    # Build WHERE clause dynamically
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

    # Count total
    count_query = f"SELECT COUNT(*) AS total FROM partner_events {where_clause}"
    cursor.execute(count_query, tuple(params))
    total = cursor.fetchone()['total']
    total_pages = math.ceil(total / per_page)

    # Fetch events
    query = f"""
        SELECT * FROM partner_events
        {where_clause}
        ORDER BY start_date DESC
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, tuple(params + [per_page, offset]))
    events = cursor.fetchall()
    conn.close()

    # ✅ Add pagination window info to JSON (optional but useful for frontend)
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
# Run the App
# ---------------------------
if __name__ == "__main__":
    app.run(debug=True)
