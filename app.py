from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from flask_cors import CORS
import math
import mysql.connector
import os
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from email_validator import validate_email, EmailNotValidError

# Load .env.dev ONLY in local dev
if os.getenv("APP_ENV") != "prod":
    from dotenv import load_dotenv
    load_dotenv(".env.dev", override=True)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-fallback-key")
CORS(app)

# ---------------------------
# Login manager
# ---------------------------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

# ---------------------------
# Database Connection
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
# User model (Flask-Login)
# ---------------------------
class User(UserMixin):
    def __init__(self, user_id: int, email: str):
        self.id = str(user_id)
        self.email = email

def get_user_by_id(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, email FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def get_user_by_email(email: str):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, email, password_hash FROM users WHERE email = %s", (email,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

@login_manager.user_loader
def load_user(user_id):
    row = get_user_by_id(int(user_id))
    if not row:
        return None
    return User(row["id"], row["email"])

# ---------------------------
# Auth routes
# ---------------------------
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()

        try:
            validate_email(email)
        except EmailNotValidError as e:
            flash(f"Invalid email: {str(e)}", "error")
            return render_template("register.html")

        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return render_template("register.html")

        if get_user_by_email(email):
            flash("Email already registered. Please log in.", "error")
            return redirect(url_for("login"))

        password_hash = generate_password_hash(password)

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO users (email, password_hash) VALUES (%s, %s)", (email, password_hash))
        conn.commit()
        cur.close()
        conn.close()

        flash("Account created! Please log in.", "success")
        return redirect(url_for("login"))

    return render_template("register.html")


# @app.route("/register", methods=["GET", "POST"])
# def register():
#     return ("Registration is disabled.", 404)


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()

        user_row = get_user_by_email(email)
        if not user_row or not check_password_hash(user_row["password_hash"], password):
            flash("Invalid email or password.", "error")
            return render_template("login.html")

        login_user(User(user_row["id"], user_row["email"]))
        flash("Logged in successfully.", "success")
        return redirect(url_for("index"))

    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.", "success")
    return redirect(url_for("index"))

# ---------------------------
# Manual add event (protected)
# ---------------------------
@app.route("/events/new", methods=["GET", "POST"])
@login_required
def add_event():
    if request.method == "POST":
        category = (request.form.get("category") or "").strip()
        title = (request.form.get("title") or "").strip()
        description = (request.form.get("description") or "").strip() or None
        start_date = (request.form.get("start_date") or "").strip()
        end_date = (request.form.get("end_date") or "").strip() or None
        location = (request.form.get("location") or "").strip() or None
        register_link = (request.form.get("register_link") or "").strip() or None

        if not title or not start_date:
            flash("Title and Start Date are required.", "error")
            return render_template("add_event.html")

        # Convert datetime-local -> MySQL DATETIME
        def normalize_dt(s):
            s = (s or "").strip()
            if not s:
                return None
            if "T" in s:
                s = s.replace("T", " ")
            if len(s) == 16:  # YYYY-MM-DD HH:MM
                s = s + ":00"
            return s

        start_dt = normalize_dt(start_date)
        end_dt = normalize_dt(end_date) if end_date else None

        conn = get_db_connection()
        cur = conn.cursor()

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

        cur.execute(insert_query, (
            "Manual",
            category or "Manual",
            title,
            description,
            start_dt,
            end_dt,
            location,
            register_link
        ))

        conn.commit()
        cur.close()
        conn.close()

        flash("Event saved!", "success")
        return redirect(url_for("index"))

    return render_template("add_event.html")

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
        """
        SELECT * FROM partner_events
        ORDER BY updated_at DESC
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
        end_page=end_page,
        current_user=current_user
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
        ORDER BY updated_at DESC
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
        "filters": {"q": q, "source": source, "category": category},
        "events": events
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
