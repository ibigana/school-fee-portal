import os
from dotenv import load_dotenv
load_dotenv()
import csv
import io
import hmac
import base64
import hashlib
import logging
import psycopg
from psycopg.rows import dict_row
import uuid
from functools import wraps
from datetime import datetime

import requests
from werkzeug.utils import secure_filename
from flask import (
    Flask,
    render_template_string,
    request,
    redirect,
    url_for,
    flash,
    send_file,
    session,
    jsonify,
)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-this-secret-key-now")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = False if os.getenv("APP_ENV", "development") == "development" else True

DATABASE_URL = os.getenv("DATABASE_URL", "")
PAYSTACK_SECRET_KEY = os.getenv("PAYSTACK_SECRET_KEY", "")
PAYSTACK_PUBLIC_KEY = os.getenv("PAYSTACK_PUBLIC_KEY", "")
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:5000").rstrip("/")
APP_ENV = os.getenv("APP_ENV", "development")
PAYSTACK_API_BASE = "https://api.paystack.co"
TIMEOUT = 30
DEFAULT_SCHOOL_NAME = "School Fee Tracker Pro"
DEFAULT_CURRENCY = "NGN"
ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
MAX_LOGO_SIZE = 2 * 1024 * 1024

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{{ school_name }}</title>
    <style>
        * { box-sizing: border-box; }
        body { font-family: Arial, sans-serif; margin: 0; background: #f5f7fb; color: #222; }
        header {
            background: linear-gradient(135deg, #123a5c 0%, #1f4e79 55%, #2c6da3 100%);
            color: white;
            padding: 16px 24px;
            position: sticky;
            top: 0;
            z-index: 1000;
            box-shadow: 0 2px 10px rgba(0,0,0,0.12);
            backdrop-filter: blur(6px);
        }
        nav a {
            color: white;
            text-decoration: none;
            font-weight: bold;
            transition: background 0.2s ease, transform 0.2s ease;
        }
        nav a:hover { transform: translateY(-1px); }
        .nav-toggle {
            display: none;
            background: transparent;
            border: 1px solid rgba(255,255,255,0.35);
            color: white;
            padding: 8px 12px;
            border-radius: 8px;
            font-size: 14px;
            width: auto;
            margin: 0;
        }
        .nav-top {
            display: grid;
            grid-template-columns: 1fr auto 1fr;
            align-items: center;
            gap: 16px;
        }
        .brand-wrap {
            display: flex;
            align-items: center;
            gap: 10px;
            min-width: 0;
        }
        .brand-wrap h2 {
            margin: 0;
            font-size: 1.25rem;
            overflow-wrap: anywhere;
        }
        .nav-spacer { justify-self: end; }
        .nav-menu {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            flex-wrap: wrap;
            margin-top: 12px;
        }
        .nav-menu a {
            padding: 10px 14px;
            border-radius: 10px;
            background: rgba(255,255,255,0.08);
        }
        .container { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
        .hero {
            background: linear-gradient(135deg, #ffffff 0%, #eef5fb 100%);
            border-radius: 18px;
            padding: 28px;
            box-shadow: 0 8px 24px rgba(18,58,92,0.08);
            margin-bottom: 24px;
            border: 1px solid rgba(31,78,121,0.08);
        }
        .hero h1 { margin-bottom: 8px; font-size: clamp(1.8rem, 4vw, 2.8rem); }
        .hero p { margin: 0; color: #4c5f73; font-size: 1rem; }
        .hero-actions { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 18px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 24px; align-items: stretch; }
        .dashboard-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; margin-bottom: 24px; align-items: stretch; }
        .card {
            background: white;
            border-radius: 16px;
            padding: 18px;
            box-shadow: 0 6px 18px rgba(0,0,0,0.06);
            min-width: 0;
            overflow: hidden;
            border: 1px solid rgba(31,78,121,0.07);
        }
        .card:hover { box-shadow: 0 10px 24px rgba(0,0,0,0.08); }
        h1, h2, h3, h4 { margin-top: 0; overflow-wrap: anywhere; word-break: break-word; }
        .stat-number {
            font-size: clamp(1.2rem, 2.8vw, 2.2rem);
            line-height: 1.15;
            margin: 0;
            overflow-wrap: anywhere;
            word-break: break-word;
        }
        .stat-label {
            margin-bottom: 10px;
            color: #5d6d7e;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            font-size: 0.82rem;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            border-radius: 14px;
            overflow: hidden;
        }
        th, td { padding: 12px; border-bottom: 1px solid #e7ebf0; text-align: left; font-size: 14px; }
        th { background: #eaf1f8; color: #284a6b; }
        form {
            background: white;
            padding: 18px;
            border-radius: 14px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            margin-bottom: 24px;
            border: 1px solid rgba(31,78,121,0.07);
        }
        input, select, button, textarea {
            width: 100%;
            padding: 12px;
            margin-top: 6px;
            margin-bottom: 14px;
            border: 1px solid #ccd5df;
            border-radius: 10px;
            box-sizing: border-box;
        }
        input:focus, select:focus, textarea:focus { outline: none; border-color: #2c6da3; box-shadow: 0 0 0 3px rgba(44,109,163,0.12); }
        button, .btn {
            background: linear-gradient(135deg, #1f4e79 0%, #2c6da3 100%);
            color: white;
            border: none;
            cursor: pointer;
            font-weight: bold;
            text-decoration: none;
            display: inline-block;
            width: auto;
            padding: 10px 16px;
            border-radius: 10px;
            box-shadow: 0 6px 14px rgba(31,78,121,0.18);
        }
        button:hover, .btn:hover { filter: brightness(1.03); }
        .btn-secondary { background: linear-gradient(135deg, #267b42 0%, #35a95a 100%); }
        .btn-warning { background: linear-gradient(135deg, #b87500 0%, #de980d 100%); }
        .btn-danger { background: linear-gradient(135deg, #9a1029 0%, #c21d3b 100%); }
        .row { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; }
        .flash {
            background: #dff0d8;
            color: #2d572c;
            padding: 12px;
            border-radius: 10px;
            margin-bottom: 16px;
            border-left: 4px solid #2f7d32;
        }
        .danger { color: #b00020; font-weight: bold; }
        .success { color: #2f7d32; font-weight: bold; }
        .muted { color: #666; font-size: 14px; }
        .top-actions { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
        .top-actions a {
            text-decoration: none;
            background: linear-gradient(135deg, #1f4e79 0%, #2c6da3 100%);
            color: #fff;
            padding: 10px 14px;
            border-radius: 10px;
            box-shadow: 0 6px 14px rgba(31,78,121,0.18);
        }
        .auth-box { max-width: 460px; margin: 50px auto; }
        .receipt { background: white; padding: 24px; border-radius: 14px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); max-width: 820px; margin: 0 auto; }
        .receipt table { box-shadow: none; }
        .badge { display: inline-block; padding: 6px 10px; border-radius: 999px; font-size: 12px; font-weight: bold; background: #eaf1f8; }
        .status-paid { background: #dff0d8; color: #2d572c; }
        .status-pending { background: #fff3cd; color: #7a5b00; }
        .status-failed { background: #f8d7da; color: #842029; }
        .school-brand { display: flex; align-items: center; gap: 16px; margin-bottom: 18px; }
        .school-brand img { width: 80px; height: 80px; object-fit: contain; border-radius: 8px; background: #fff; border: 1px solid #ddd; }
        .mini-logo { width: 42px; height: 42px; object-fit: contain; border-radius: 6px; background: white; }
        .split { display: grid; grid-template-columns: 2fr 1fr; gap: 18px; }
        .icon-chip { display: inline-flex; align-items: center; justify-content: center; width: 38px; height: 38px; border-radius: 12px; background: linear-gradient(135deg, #eaf1f8 0%, #dbeaf6 100%); font-size: 18px; margin-bottom: 10px; }
        .landing-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 18px; margin-top: 22px; }
        .right { text-align: right; }
        @media (max-width: 1024px) {
            .dashboard-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
        }
        @media (max-width: 850px) {
            .split { grid-template-columns: 1fr; }
        }
        @media (max-width: 768px) {
            header { padding: 14px 16px; }
            .container { padding: 0 12px; }
            .nav-top { grid-template-columns: 1fr auto; }
            .nav-spacer { display: none; }
            .nav-toggle { display: inline-block; }
            .nav-menu {
                display: none;
                width: 100%;
                flex-direction: column;
                align-items: stretch;
                gap: 8px;
                margin-top: 12px;
                background: rgba(255,255,255,0.08);
                padding: 12px;
                border-radius: 12px;
            }
            .nav-menu.show { display: flex; }
            nav a {
                display: block;
                text-align: center;
            }
            .top-actions { flex-direction: column; }
            .top-actions a, .btn { width: 100%; text-align: center; }
            table { display: block; overflow-x: auto; white-space: nowrap; }
            .school-brand { align-items: flex-start; }
            .hero { padding: 22px 18px; }
            .hero-actions { flex-direction: column; }
            .dashboard-grid { grid-template-columns: 1fr; }
        }
        @media print {
            header, nav, .top-actions, .no-print { display: none !important; }
            body { background: white; }
            .container { max-width: 100%; margin: 0; padding: 0; }
            .receipt { box-shadow: none; border-radius: 0; max-width: 100%; }
        }
    </style>
    <script>
        function toggleMenu() {
            const menu = document.getElementById('mobileNavMenu');
            if (menu) menu.classList.toggle('show');
        }
    </script>
</head>
<body>
    {% if request.endpoint == 'landing_page' %}
    <div class="container" style="padding-top:32px;">
        {{ content|safe }}
    </div>
    {% elif session.get('user_id') %}
    <header>
        <div style="margin-bottom:12px;">
            <form action="{{ url_for('global_search') }}" method="get" class="no-print" style="background:transparent;box-shadow:none;padding:0;margin:0;border:none;">
                <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;">
                    <input type="text" name="q" placeholder="Search student by name, student ID, class, parent email, or payment reference" style="flex:1;min-width:240px;margin:0;background:white;" value="{{ request.args.get('q','') if request.endpoint == 'global_search' else '' }}">
                    <button type="submit" class="btn" style="margin:0;">Search</button>
                </div>
            </form>
        </div>
        <div class="nav-top">
            <div class="brand-wrap">
                {% if school_logo %}<img src="{{ school_logo }}" class="mini-logo" alt="Logo">{% endif %}
                <h2>{{ school_name }}</h2>
            </div>
            <button class="nav-toggle" onclick="toggleMenu()">Menu</button>
            <div class="nav-spacer"></div>
        </div>
        <nav id="mobileNavMenu" class="nav-menu">
            {% if session.get('role') == 'admin' %}
                <a href="{{ url_for('dashboard') }}">Dashboard</a>
                <a href="{{ url_for('students') }}">Students</a>
                <a href="{{ url_for('parents_page') }}">Parents</a>
                <a href="{{ url_for('payments') }}">Payments</a>
                <a href="{{ url_for('reports') }}">Reports</a>
                <a href="{{ url_for('settings_page') }}">Settings</a>
                <a href="{{ url_for('admin_accounts_page') }}">Admin Accounts</a>
            {% else %}
                <a href="{{ url_for('parent_dashboard') }}">My Dashboard</a>
                <a href="{{ url_for('parent_children') }}">My Children</a>
                <a href="{{ url_for('parent_payments') }}">My Payments</a>
            {% endif %}
            <a href="{{ url_for('logout') }}">Logout</a>
        </nav>
    </header>
    <div class="container">
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for message in messages %}
                    <div class="flash">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        {{ content|safe }}
    </div>
    {% else %}
    <div class="container">
        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for message in messages %}
                    <div class="flash">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        {{ content|safe }}
    </div>
    {% endif %}
</body>
</html>
"""


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing. Set it in your environment variables.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def generate_reference(prefix: str = "TXN") -> str:
    return f"{prefix}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"


def generate_student_code(conn) -> str:
    year_part = datetime.now().strftime('%Y')
    last_row = conn.execute(
        "SELECT student_code FROM students WHERE student_code LIKE ? ORDER BY id DESC LIMIT 1",
        (f"STD-{year_part}-%",),
    ).fetchone()
    next_no = 1
    if last_row and last_row["student_code"]:
        try:
            next_no = int(last_row["student_code"].split("-")[-1]) + 1
        except (ValueError, IndexError):
            next_no = conn.execute("SELECT COUNT(*) AS total FROM students").fetchone()["total"] + 1
    return f"STD-{year_part}-{next_no:05d}"


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_setting(conn, key, default=""):
    row = conn.execute("SELECT value FROM settings WHERE key_name = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn, key, value):
    existing = conn.execute("SELECT id FROM settings WHERE key_name = ?", (key,)).fetchone()
    if existing:
        conn.execute("UPDATE settings SET value = ? WHERE key_name = ?", (value, key))
    else:
        conn.execute("INSERT INTO settings (key_name, value) VALUES (?, ?)", (key, value))


def school_context():
    conn = get_db_connection()
    school_name = get_setting(conn, "school_name", DEFAULT_SCHOOL_NAME)
    school_logo = get_setting(conn, "school_logo", "")
    conn.close()
    return school_name, school_logo


def allowed_logo_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS


def file_to_data_url(file_storage):
    if not file_storage or not file_storage.filename:
        return ""
    filename = secure_filename(file_storage.filename)
    if not allowed_logo_file(filename):
        raise ValueError("Only PNG, JPG, JPEG, WEBP, and GIF logo files are allowed.")
    file_bytes = file_storage.read()
    if len(file_bytes) > MAX_LOGO_SIZE:
        raise ValueError("Logo file is too large. Maximum size is 2MB.")
    extension = filename.rsplit(".", 1)[1].lower()
    mime_map = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "webp": "image/webp",
        "gif": "image/gif",
    }
    mime_type = mime_map.get(extension, "image/png")
    encoded = base64.b64encode(file_bytes).decode("utf-8")
    return f"data:{mime_type};base64,{encoded}"


def render_page(content: str):
    school_name, school_logo = school_context()
    return render_template_string(BASE_HTML, content=content, school_name=school_name, school_logo=school_logo)


def current_user():
    if session.get("user_id"):
        return {
            "id": session.get("user_id"),
            "username": session.get("username"),
            "role": session.get("role"),
        }
    return None


def login_required(role=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                flash("Please login first.")
                return redirect(url_for("login"))
            if role and user.get("role") != role:
                flash("You are not allowed to access that page.")
                if user.get("role") == "parent":
                    return redirect(url_for("parent_dashboard"))
                return redirect(url_for("dashboard"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'admin',
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS parents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            phone TEXT,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_code TEXT UNIQUE,
            full_name TEXT NOT NULL,
            class_name TEXT NOT NULL,
            parent_phone TEXT,
            parent_email TEXT,
            parent_id INTEGER,
            total_fee REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            FOREIGN KEY(parent_id) REFERENCES parents(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER NOT NULL,
            amount_paid REAL NOT NULL,
            payment_date TEXT,
            term_name TEXT NOT NULL,
            method TEXT NOT NULL DEFAULT 'Cash',
            status TEXT NOT NULL DEFAULT 'Pending',
            reference TEXT NOT NULL UNIQUE,
            note TEXT,
            gateway_response TEXT,
            paystack_reference TEXT,
            paystack_access_code TEXT,
            channel TEXT,
            paid_at TEXT,
            created_by INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            FOREIGN KEY(student_id) REFERENCES students(id),
            FOREIGN KEY(created_by) REFERENCES users(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            event_reference TEXT,
            payload TEXT NOT NULL,
            received_at TEXT NOT NULL,
            UNIQUE(event_type, event_reference)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_name TEXT NOT NULL UNIQUE,
            value TEXT
        )
        """
    )

    conn.commit()

    existing_student_cols = [row[1] for row in cur.execute("PRAGMA table_info(students)").fetchall()]
    if "student_code" not in existing_student_cols:
        cur.execute("ALTER TABLE students ADD COLUMN student_code TEXT")
        conn.commit()
    if "parent_id" not in existing_student_cols:
        cur.execute("ALTER TABLE students ADD COLUMN parent_id INTEGER")
        conn.commit()
    if "parent_email" not in existing_student_cols:
        cur.execute("ALTER TABLE students ADD COLUMN parent_email TEXT")
        conn.commit()
    if "parent_phone" not in existing_student_cols:
        cur.execute("ALTER TABLE students ADD COLUMN parent_phone TEXT")
        conn.commit()

    existing_students = cur.execute(
        "SELECT id FROM students WHERE student_code IS NULL OR student_code = '' ORDER BY id ASC"
    ).fetchall()
    for row in existing_students:
        cur.execute("UPDATE students SET student_code = ? WHERE id = ?", (generate_student_code(conn), row[0]))
    conn.commit()

    admin = cur.execute("SELECT id FROM users WHERE username = ?", ("admin",)).fetchone()
    if not admin:
        cur.execute(
            "INSERT INTO users (full_name, username, password_hash, role, created_at) VALUES (?, ?, ?, ?, ?)",
            ("System Administrator", "admin", hash_password("admin123"), "admin", now_str()),
        )

    set_setting(conn, "school_name", get_setting(conn, "school_name", DEFAULT_SCHOOL_NAME))
    set_setting(conn, "school_logo", get_setting(conn, "school_logo", ""))
    set_setting(conn, "currency", get_setting(conn, "currency", DEFAULT_CURRENCY))
    conn.commit()
    conn.close()


def currency_symbol():
    conn = get_db_connection()
    code = get_setting(conn, "currency", DEFAULT_CURRENCY)
    conn.close()
    return "₦" if code == "NGN" else code + " "


def paystack_headers():
    if not PAYSTACK_SECRET_KEY:
        raise RuntimeError("PAYSTACK_SECRET_KEY is missing. Set it in your environment variables.")
    return {
        "Authorization": f"Bearer {PAYSTACK_SECRET_KEY}",
        "Content-Type": "application/json",
    }


def safe_amount_to_kobo(amount_naira: float) -> int:
    return int(round(float(amount_naira) * 100))


def get_student_balance(conn, student_id):
    student = conn.execute("SELECT total_fee FROM students WHERE id = ?", (student_id,)).fetchone()
    if not student:
        return None
    paid = conn.execute(
        "SELECT COALESCE(SUM(amount_paid), 0) AS total FROM payments WHERE student_id = ? AND status = 'Paid'",
        (student_id,),
    ).fetchone()["total"]
    return float(student["total_fee"] - paid)


def parent_total_balance(conn, parent_id):
    rows = conn.execute("SELECT id FROM students WHERE parent_id = ?", (parent_id,)).fetchall()
    return sum(get_student_balance(conn, row["id"]) or 0 for row in rows)


def initialize_paystack_transaction(student, amount_paid, term_name, local_reference, note=""):
    callback_url = f"{BASE_URL}{url_for('paystack_callback')}"
    payload = {
        "email": student["parent_email"] or f"student-{student['id']}@example.com",
        "amount": safe_amount_to_kobo(amount_paid),
        "currency": DEFAULT_CURRENCY,
        "reference": local_reference,
        "callback_url": callback_url,
        "metadata": {
            "student_id": student["id"],
            "student_name": student["full_name"],
            "class_name": student["class_name"],
            "term_name": term_name,
            "local_reference": local_reference,
            "note": note,
        },
    }
    response = requests.post(
        f"{PAYSTACK_API_BASE}/transaction/initialize",
        headers=paystack_headers(),
        json=payload,
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def verify_paystack_transaction(reference):
    response = requests.get(
        f"{PAYSTACK_API_BASE}/transaction/verify/{reference}",
        headers=paystack_headers(),
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def signature_is_valid(raw_body: bytes, signature: str) -> bool:
    if not PAYSTACK_SECRET_KEY or not signature:
        return False
    computed = hmac.new(PAYSTACK_SECRET_KEY.encode("utf-8"), raw_body, hashlib.sha512).hexdigest()
    return hmac.compare_digest(computed, signature)


def mark_payment_success(conn, payment_row, verified_data):
    status = verified_data.get("status")
    gateway_response = verified_data.get("gateway_response") or verified_data.get("message") or ""
    channel = verified_data.get("channel") or ""
    paid_at = verified_data.get("paid_at") or now_str()
    amount_kobo = verified_data.get("amount", 0)
    expected_kobo = safe_amount_to_kobo(payment_row["amount_paid"])

    if status != "success":
        conn.execute(
            "UPDATE payments SET status = ?, gateway_response = ?, channel = ?, updated_at = ? WHERE id = ?",
            ("Failed", gateway_response, channel, now_str(), payment_row["id"]),
        )
        conn.commit()
        return False, f"Verification returned non-success status: {status}"

    if int(amount_kobo) != int(expected_kobo):
        conn.execute(
            "UPDATE payments SET status = ?, gateway_response = ?, channel = ?, updated_at = ? WHERE id = ?",
            ("Failed", f"Amount mismatch. Verified {amount_kobo} kobo", channel, now_str(), payment_row["id"]),
        )
        conn.commit()
        return False, "Amount mismatch during verification."

    conn.execute(
        "UPDATE payments SET status = ?, payment_date = ?, paid_at = ?, gateway_response = ?, channel = ?, updated_at = ? WHERE id = ?",
        ("Paid", paid_at[:10], paid_at, gateway_response, channel, now_str(), payment_row["id"]),
    )
    conn.commit()
    return True, "Payment verified successfully."


def upsert_webhook_event(conn, event_type, event_reference, payload_text):
    try:
        conn.execute(
            "INSERT INTO webhook_events (event_type, event_reference, payload, received_at) VALUES (?, ?, ?, ?)",
            (event_type, event_reference, payload_text, now_str()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def ensure_parent_can_access_student(conn, parent_id, student_id):
    row = conn.execute("SELECT id FROM students WHERE id = ? AND parent_id = ?", (student_id, parent_id)).fetchone()
    return row is not None


@app.route('/')
def landing_page():
    if current_user():
        if session.get('role') == 'parent':
            return redirect(url_for('parent_dashboard'))
        return redirect(url_for('dashboard'))

    school_name, school_logo = school_context()
    content = f"""
    <div class='hero' style='padding:38px 30px;'>
        <div style='display:flex;align-items:center;gap:18px;flex-wrap:wrap;margin-bottom:18px;'>
            {f"<img src='{school_logo}' alt='School Logo' style='width:90px;height:90px;object-fit:contain;border-radius:14px;background:#fff;border:1px solid #ddd;padding:8px;'>" if school_logo else "<div class='icon-chip' style='width:72px;height:72px;font-size:28px;'>🏫</div>"}
            <div>
                <h1 style='margin-bottom:6px;'>{school_name}</h1>
                <p>Welcome to the digital school portal for parent access, student dashboards, receipts, and secure online fee payments.</p>
            </div>
        </div>
        <div class='hero-actions'>
            <a href='{url_for('login')}' class='btn'>Parent / Admin Login</a>
            <a href='{url_for('health')}' class='btn btn-secondary'>Portal Status</a>
        </div>
    </div>

    <div class='landing-grid'>
        <div class='card'>
            <div class='icon-chip'>👨‍👩‍👧</div>
            <h3>For Parents</h3>
            <p class='muted'>Log in to see your children, track fees, print receipts, and make online payments safely.</p>
        </div>
        <div class='card'>
            <div class='icon-chip'>🏫</div>
            <h3>For Schools</h3>
            <p class='muted'>Manage parent accounts, student records, balances, branding, and real-time financial reports.</p>
        </div>
        <div class='card'>
            <div class='icon-chip'>💳</div>
            <h3>Secure Payments</h3>
            <p class='muted'>Accept school fees online through Paystack with transaction verification and receipt generation.</p>
        </div>
    </div>

    <div class='split' style='margin-top:24px;'>
        <div class='card'>
            <h3>Why this portal helps</h3>
            <ul style='margin:0;padding-left:18px;line-height:1.8;'>
                <li>Parents can monitor all linked children in one account</li>
                <li>Schools can upload logos and customize receipts</li>
                <li>Balances and payment history update automatically</li>
                <li>Works for manual payments and Paystack online payments</li>
            </ul>
        </div>
        <div class='card'>
            <h3>Quick access</h3>
            <p class='muted'>Already have an account?</p>
            <div class='hero-actions' style='margin-top:10px;'>
                <a href='{url_for('login')}' class='btn'>Open Login</a>
            </div>
        </div>
    </div>
    """
    return render_page(content)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user():
        if session.get('role') == 'parent':
            return redirect(url_for('parent_dashboard'))
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        user_type = request.form['user_type'].strip()
        username = request.form['username'].strip()
        password = request.form['password'].strip()
        conn = get_db_connection()

        if user_type == 'admin':
            user = conn.execute(
                "SELECT * FROM users WHERE username = ? AND password_hash = ?",
                (username, hash_password(password)),
            ).fetchone()
            conn.close()
            if user:
                session['user_id'] = user['id']
                session['username'] = user['username']
                session['role'] = 'admin'
                flash('Admin login successful.')
                return redirect(url_for('dashboard'))
        else:
            parent = conn.execute(
                "SELECT * FROM parents WHERE email = ? AND password_hash = ?",
                (username, hash_password(password)),
            ).fetchone()
            conn.close()
            if parent:
                session['user_id'] = parent['id']
                session['username'] = parent['email']
                session['role'] = 'parent'
                flash('Parent login successful.')
                return redirect(url_for('parent_dashboard'))

        flash('Invalid login details.')
        return redirect(url_for('login'))

    content = """
    <div class='auth-box'>
        <form method='post'>
            <div class='icon-chip'>🔐</div><h2>Login</h2>
            <label>User Type</label>
            <select name='user_type' required>
                <option value='admin'>School Admin</option>
                <option value='parent'>Parent</option>
            </select>
            <label>Username / Email</label>
            <input type='text' name='username' required>
            <label>Password</label>
            <input type='password' name='password' required>
            <button type='submit'>Login</button>
            <p class='muted'>Default admin login: <strong>admin</strong> / <strong>admin123</strong></p>
        </form>
    </div>
    """
    return render_page(content)


@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.')
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required('admin')
def dashboard():
    conn = get_db_connection()
    total_students = conn.execute("SELECT COUNT(*) AS count FROM students").fetchone()["count"]
    total_parents = conn.execute("SELECT COUNT(*) AS count FROM parents").fetchone()["count"]
    total_expected = conn.execute("SELECT COALESCE(SUM(total_fee), 0) AS total FROM students").fetchone()["total"]
    total_received = conn.execute("SELECT COALESCE(SUM(amount_paid), 0) AS total FROM payments WHERE status = 'Paid'").fetchone()["total"]
    total_pending = conn.execute("SELECT COALESCE(SUM(amount_paid), 0) AS total FROM payments WHERE status = 'Pending'").fetchone()["total"]
    outstanding = total_expected - total_received
    symbol = currency_symbol()

    recent_payments = conn.execute(
        """
        SELECT p.id, s.student_code, s.full_name, p.amount_paid, p.payment_date, p.term_name, p.reference, p.status, p.method, p.channel
        FROM payments p
        JOIN students s ON s.id = p.student_id
        ORDER BY p.id DESC LIMIT 10
        """
    ).fetchall()
    conn.close()

    rows = "".join(
        f"<tr><td>{p['student_code'] or '-'}</td><td>{p['full_name']}</td><td>{symbol}{p['amount_paid']:,.2f}</td><td>{p['term_name']}</td><td>{p['method']}</td><td>{p['channel'] or '-'}</td><td>{p['status']}</td><td>{p['reference']}</td><td>{p['payment_date'] or '-'}</td></tr>"
        for p in recent_payments
    ) or "<tr><td colspan='9'>No payments yet.</td></tr>"

    content = f"""
    <div class='hero'>
        <h1>School Finance Control Center</h1>
        <p>Manage parents, students, receipts, and online school fee payments from one professional dashboard.</p>
        <div class='hero-actions'>
            <a href='{url_for('students')}' class='btn'>Add Student</a>
            <a href='{url_for('parents_page')}' class='btn btn-secondary'>Manage Parents</a>
            <a href='{url_for('settings_page')}' class='btn btn-warning'>School Branding</a>
        </div>
    </div>
    <div class='dashboard-grid'>
        <div class='card'><div class='icon-chip'>🎓</div><h3 class='stat-label'>Total Students</h3><h1 class='stat-number'>{total_students}</h1></div>
        <div class='card'><div class='icon-chip'>👨‍👩‍👧</div><h3 class='stat-label'>Total Parents</h3><h1 class='stat-number'>{total_parents}</h1></div>
        <div class='card'><div class='icon-chip'>💼</div><h3 class='stat-label'>Total Expected Fees</h3><h1 class='stat-number'>{symbol}{total_expected:,.2f}</h1></div>
        <div class='card'><div class='icon-chip'>✅</div><h3 class='stat-label'>Total Received</h3><h1 class='stat-number success'>{symbol}{total_received:,.2f}</h1></div>
        <div class='card'><div class='icon-chip'>⏳</div><h3 class='stat-label'>Pending</h3><h1 class='stat-number'>{symbol}{total_pending:,.2f}</h1></div>
        <div class='card'><div class='icon-chip'>📌</div><h3 class='stat-label'>Outstanding</h3><h1 class='stat-number danger'>{symbol}{outstanding:,.2f}</h1></div>
    </div>

    <div class='top-actions'>
        <a href='{url_for('students')}'>Add Student</a>
        <a href='{url_for('parents_page')}'>Manage Parents</a>
        <a href='{url_for('payments')}'>Record Manual Payment</a>
        <a href='{url_for('reports')}'>View Reports</a>
    </div>

    <h3>Recent Payments</h3>
    <table>
        <tr><th>Student ID</th><th>Student</th><th>Amount</th><th>Term</th><th>Method</th><th>Channel</th><th>Status</th><th>Reference</th><th>Date</th></tr>
        {rows}
    </table>
    """
    return render_page(content)


@app.route('/admin-accounts', methods=['GET', 'POST'])
@login_required('admin')
def admin_accounts_page():
    conn = get_db_connection()

    if request.method == 'POST':
        full_name = request.form['full_name'].strip()
        username = request.form['username'].strip()
        password = request.form['password'].strip()

        if not full_name or not username or not password:
            conn.close()
            flash('Full name, username, and password are required.')
            return redirect(url_for('admin_accounts_page'))

        try:
            conn.execute(
                "INSERT INTO users (full_name, username, password_hash, role, created_at) VALUES (%s, %s, %s, %s, %s)",
                (full_name, username, hash_password(password), 'admin', now_str()),
            )
            conn.commit()
            flash('Admin account created successfully.')
        except Exception as exc:
            conn.rollback()
            msg = str(exc)
            if 'duplicate' in msg.lower() or 'unique' in msg.lower():
                flash('That username already exists.')
            else:
                flash(f'Could not create admin account: {msg}')
        conn.close()
        return redirect(url_for('admin_accounts_page'))

    admins = conn.execute(
        "SELECT id, full_name, username, role, created_at FROM users WHERE role = %s ORDER BY id DESC",
        ('admin',)
    ).fetchall()
    conn.close()

    rows = "".join(
        f"<tr><td>{a['full_name']}</td><td>{a['username']}</td><td>{a['role']}</td><td>{a['created_at']}</td></tr>"
        for a in admins
    ) or "<tr><td colspan='4'>No admin accounts found.</td></tr>"

    content = f"""
    <div class='hero'>
        <h1>Admin Accounts</h1>
        <p>Create additional school administrator accounts for finance, bursary, or management staff.</p>
    </div>

    <form method='post'>
        <h2>Create Admin Account</h2>
        <div class='row'>
            <div><label>Full Name</label><input type='text' name='full_name' required></div>
            <div><label>Username</label><input type='text' name='username' required></div>
        </div>
        <div class='row'>
            <div><label>Password</label><input type='text' name='password' required></div>
        </div>
        <button type='submit'>Create Admin</button>
    </form>

    <h2>Existing Admin Accounts</h2>
    <table>
        <tr><th>Full Name</th><th>Username</th><th>Role</th><th>Created At</th></tr>
        {rows}
    </table>
    """
    return render_page(content)


@app.route('/parents', methods=['GET', 'POST'])
@login_required('admin')
def parents_page():
    conn = get_db_connection()

    if request.method == 'POST':
        full_name = request.form['full_name'].strip()
        email = request.form['email'].strip().lower()
        phone = request.form['phone'].strip()
        password = request.form['password'].strip()
        if not full_name or not email or not password:
            conn.close()
            flash('Parent name, email, and password are required.')
            return redirect(url_for('parents_page'))
        try:
            conn.execute(
                "INSERT INTO parents (full_name, email, phone, password_hash, created_at) VALUES (?, ?, ?, ?, ?)",
                (full_name, email, phone, hash_password(password), now_str()),
            )
            conn.commit()
            flash('Parent account created successfully.')
        except sqlite3.IntegrityError:
            flash('That parent email already exists.')
        conn.close()
        return redirect(url_for('parents_page'))

    parents = conn.execute(
        "SELECT p.*, (SELECT COUNT(*) FROM students s WHERE s.parent_id = p.id) AS child_count FROM parents p ORDER BY p.id DESC"
    ).fetchall()
    conn.close()

    rows = "".join(
        f"<tr><td>{p['full_name']}</td><td>{p['email']}</td><td>{p['phone'] or '-'}</td><td>{p['child_count']}</td></tr>"
        for p in parents
    ) or "<tr><td colspan='4'>No parent accounts yet.</td></tr>"

    content = f"""
    <h2>Create Parent Account</h2>
    <form method='post'>
        <div class='row'>
            <div><label>Full Name</label><input type='text' name='full_name' required></div>
            <div><label>Email</label><input type='email' name='email' required></div>
        </div>
        <div class='row'>
            <div><label>Phone</label><input type='text' name='phone'></div>
            <div><label>Password</label><input type='text' name='password' placeholder='Create login password' required></div>
        </div>
        <button type='submit'>Save Parent Account</button>
    </form>

    <h2>Parent Accounts</h2>
    <table>
        <tr><th>Name</th><th>Email</th><th>Phone</th><th>Children</th></tr>
        {rows}
    </table>
    """
    return render_page(content)


@app.route('/students', methods=['GET', 'POST'])
@login_required('admin')
def students():
    conn = get_db_connection()
    symbol = currency_symbol()

    if request.method == 'POST':
        full_name = request.form['full_name'].strip()
        class_name = request.form['class_name'].strip()
        parent_phone = request.form['parent_phone'].strip()
        parent_email = request.form['parent_email'].strip().lower()
        parent_id = request.form['parent_id'].strip()
        total_fee = request.form['total_fee'].strip()

        if not full_name or not class_name or not total_fee:
            conn.close()
            flash('Please fill in Full Name, Class, and Total Fee.')
            return redirect(url_for('students'))

        parent_id_value = int(parent_id) if parent_id else None
        if parent_id_value and not parent_email:
            parent_row = conn.execute("SELECT email, phone FROM parents WHERE id = ?", (parent_id_value,)).fetchone()
            if parent_row:
                parent_email = parent_row['email'] or parent_email
                parent_phone = parent_phone or (parent_row['phone'] or '')

        conn.execute(
            "INSERT INTO students (student_code, full_name, class_name, parent_phone, parent_email, parent_id, total_fee, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (generate_student_code(conn), full_name, class_name, parent_phone, parent_email, parent_id_value, float(total_fee), now_str()),
        )
        conn.commit()
        conn.close()
        flash('Student added successfully.')
        return redirect(url_for('students'))

    parent_options = conn.execute("SELECT id, full_name, email FROM parents ORDER BY full_name ASC").fetchall()
    student_rows = conn.execute(
        """
        SELECT s.*, p.full_name AS parent_name,
               COALESCE(SUM(CASE WHEN py.status = 'Paid' THEN py.amount_paid ELSE 0 END), 0) AS amount_paid
        FROM students s
        LEFT JOIN parents p ON p.id = s.parent_id
        LEFT JOIN payments py ON s.id = py.student_id
        GROUP BY s.id
        ORDER BY s.id DESC
        """
    ).fetchall()
    conn.close()

    options = "".join(
        f"<option value='{p['id']}'>{p['full_name']} - {p['email']}</option>" for p in parent_options
    )

    rows = "".join(
        f"<tr><td>{s['student_code'] or '-'}</td><td>{s['full_name']}</td><td>{s['class_name']}</td><td>{s['parent_name'] or '-'}</td><td>{s['parent_email'] or '-'}</td><td>{symbol}{s['total_fee']:,.2f}</td><td>{symbol}{s['amount_paid']:,.2f}</td><td>{symbol}{(s['total_fee'] - s['amount_paid']):,.2f}</td><td><a class='btn btn-secondary' href='{url_for('start_paystack_payment', student_id=s['id'])}'>Pay Online</a></td></tr>"
        for s in student_rows
    ) or "<tr><td colspan='9'>No students yet.</td></tr>"

    content = f"""
    <h2>Add Student</h2>
    <form method='post'>
        <div class='row'>
            <div><label>Full Name</label><input type='text' name='full_name' required></div>
            <div><label>Class</label><input type='text' name='class_name' required></div>
        </div>
        <div class='row'>
            <div>
                <label>Link Parent Account</label>
                <select name='parent_id'>
                    <option value=''>Select Parent (optional)</option>
                    {options}
                </select>
            </div>
            <div><label>Parent Email</label><input type='email' name='parent_email'></div>
        </div>
        <div class='row'>
            <div><label>Parent Phone</label><input type='text' name='parent_phone'></div>
            <div><label>Total Fee</label><input type='number' step='0.01' name='total_fee' required></div>
        </div>
        <button type='submit'>Save Student</button>
    </form>

    <h2>Student List</h2>
    <table>
        <tr><th>Student ID</th><th>Name</th><th>Class</th><th>Parent</th><th>Parent Email</th><th>Total Fee</th><th>Paid</th><th>Balance</th><th>Online Payment</th></tr>
        {rows}
    </table>
    """
    return render_page(content)


@app.route('/payments', methods=['GET', 'POST'])
@login_required('admin')
def payments():
    conn = get_db_connection()
    symbol = currency_symbol()

    if request.method == 'POST':
        student_id = request.form['student_id']
        amount_paid = request.form['amount_paid']
        payment_date = request.form['payment_date']
        term_name = request.form['term_name'].strip()
        method = request.form['method'].strip()
        note = request.form['note'].strip()
        reference = generate_reference("RCP")

        if not student_id or not amount_paid or not payment_date or not term_name or not method:
            conn.close()
            flash('Please complete all required payment fields.')
            return redirect(url_for('payments'))

        conn.execute(
            "INSERT INTO payments (student_id, amount_paid, payment_date, term_name, method, status, reference, note, created_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (int(student_id), float(amount_paid), payment_date, term_name, method, 'Paid', reference, note, session.get('user_id'), now_str(), now_str()),
        )
        conn.commit()
        payment_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        conn.close()
        flash('Manual payment recorded successfully.')
        return redirect(url_for('receipt', payment_id=payment_id))

    student_options = conn.execute("SELECT id, student_code, full_name, class_name FROM students ORDER BY full_name ASC").fetchall()
    payment_rows = conn.execute(
        "SELECT p.id, s.student_code, s.full_name, s.class_name, p.amount_paid, p.payment_date, p.term_name, p.note, p.reference, p.method, p.status FROM payments p JOIN students s ON s.id = p.student_id ORDER BY p.id DESC"
    ).fetchall()
    conn.close()

    options = "".join(
        f"<option value='{s['id']}'>{s['student_code'] or '-'} - {s['full_name']} - {s['class_name']}</option>"
        for s in student_options
    )
    rows = "".join(
        f"<tr><td>{p['student_code'] or '-'}</td><td>{p['full_name']}</td><td>{p['class_name']}</td><td>{symbol}{p['amount_paid']:,.2f}</td><td>{p['term_name']}</td><td>{p['method']}</td><td>{p['status']}</td><td>{p['reference']}</td><td>{p['payment_date'] or '-'}</td><td><a class='btn' href='{url_for('receipt', payment_id=p['id'])}'>Receipt</a></td></tr>"
        for p in payment_rows
    ) or "<tr><td colspan='10'>No payments yet.</td></tr>"

    content = f"""
    <h2>Record Manual Payment</h2>
    <form method='post'>
        <div class='row'>
            <div><label>Student</label><select name='student_id' required><option value=''>Select Student</option>{options}</select></div>
            <div><label>Amount Paid</label><input type='number' step='0.01' name='amount_paid' required></div>
        </div>
        <div class='row'>
            <div><label>Payment Date</label><input type='date' name='payment_date' required></div>
            <div><label>Term</label><input type='text' name='term_name' required></div>
            <div><label>Method</label><select name='method' required><option value='Cash'>Cash</option><option value='Bank Transfer'>Bank Transfer</option><option value='POS'>POS</option></select></div>
        </div>
        <label>Note</label><input type='text' name='note'>
        <button type='submit'>Save Manual Payment</button>
    </form>

    <h2>Payment History</h2>
    <table>
        <tr><th>Student ID</th><th>Student</th><th>Class</th><th>Amount</th><th>Term</th><th>Method</th><th>Status</th><th>Reference</th><th>Date</th><th>Receipt</th></tr>
        {rows}
    </table>
    """
    return render_page(content)


@app.route('/parent-dashboard')
@login_required('parent')
def parent_dashboard():
    conn = get_db_connection()
    parent = conn.execute("SELECT * FROM parents WHERE id = ?", (session['user_id'],)).fetchone()
    children = conn.execute(
        """
        SELECT s.*, COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid
        FROM students s
        LEFT JOIN payments p ON p.student_id = s.id
        WHERE s.parent_id = ?
        GROUP BY s.id
        ORDER BY s.full_name ASC
        """,
        (session['user_id'],),
    ).fetchall()
    recent_payments = conn.execute(
        """
        SELECT p.*, s.student_code, s.full_name FROM payments p
        JOIN students s ON s.id = p.student_id
        WHERE s.parent_id = ?
        ORDER BY p.id DESC LIMIT 10
        """,
        (session['user_id'],),
    ).fetchall()
    total_balance = parent_total_balance(conn, session['user_id'])
    symbol = currency_symbol()
    conn.close()

    child_cards = "".join(
        f"<tr><td>{c['student_code'] or '-'}</td><td>{c['full_name']}</td><td>{c['class_name']}</td><td>{symbol}{c['total_fee']:,.2f}</td><td>{symbol}{c['amount_paid']:,.2f}</td><td>{symbol}{(c['total_fee'] - c['amount_paid']):,.2f}</td><td><a class='btn btn-secondary' href='{url_for('start_paystack_payment', student_id=c['id'])}'>Pay Now</a></td></tr>"
        for c in children
    ) or "<tr><td colspan='7'>No child linked yet.</td></tr>"

    payments_rows = "".join(
        f"<tr><td>{p['student_code'] or '-'}</td><td>{p['full_name']}</td><td>{symbol}{p['amount_paid']:,.2f}</td><td>{p['term_name']}</td><td>{p['method']}</td><td>{p['status']}</td><td>{p['payment_date'] or '-'}</td></tr>"
        for p in recent_payments
    ) or "<tr><td colspan='7'>No payments yet.</td></tr>"

    content = f"""
    <div class='grid'>
        <div class='card'><h3 class='stat-label'>Parent</h3><h2 class='stat-number'>{parent['full_name']}</h2><p>{parent['email']}</p></div>
        <div class='card'><h3 class='stat-label'>Children</h3><h1 class='stat-number'>{len(children)}</h1></div>
        <div class='card'><h3 class='stat-label'>Total Outstanding</h3><h1 class='stat-number danger'>{symbol}{total_balance:,.2f}</h1></div>
    </div>

    <div class='landing-grid'>
        <div class='card'><div class='icon-chip'>🧾</div><h3>Receipts & Branding</h3><p class='muted'>Generate clean receipts with the school logo and school name automatically applied.</p></div>
        <div class='card'><div class='icon-chip'>💳</div><h3>Parent Online Payments</h3><p class='muted'>Parents can log in, check balances, and pay securely through Paystack.</p></div>
        <div class='card'><div class='icon-chip'>📊</div><h3>Live Dashboards</h3><p class='muted'>Track expected fees, paid fees, pending transactions, and outstanding balances in one view.</p></div>
    </div>

    <div class='split'>
        <div>
            <h3>My Children</h3>
            <table>
                <tr><th>Student ID</th><th>Name</th><th>Class</th><th>Total Fee</th><th>Paid</th><th>Balance</th><th>Action</th></tr>
                {child_cards}
            </table>
        </div>
        <div class='card'>
            <h3>Quick Help</h3>
            <p>Parents can view children, balances, receipts, and pay school fees online.</p>
            <p class='muted'>Each child payment uses Paystack and automatically updates the dashboard after verification.</p>
        </div>
    </div>

    <h3 style='margin-top:20px;'>Recent Payments</h3>
    <table>
        <tr><th>Student ID</th><th>Child</th><th>Amount</th><th>Term</th><th>Method</th><th>Status</th><th>Date</th></tr>
        {payments_rows}
    </table>
    """
    return render_page(content)


@app.route('/parent-children')
@login_required('parent')
def parent_children():
    conn = get_db_connection()
    symbol = currency_symbol()
    children = conn.execute(
        """
        SELECT s.*, COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid
        FROM students s
        LEFT JOIN payments p ON p.student_id = s.id
        WHERE s.parent_id = ?
        GROUP BY s.id
        ORDER BY s.full_name ASC
        """,
        (session['user_id'],),
    ).fetchall()
    conn.close()

    rows = "".join(
        f"<tr><td>{c['student_code'] or '-'}</td><td>{c['full_name']}</td><td>{c['class_name']}</td><td>{symbol}{c['total_fee']:,.2f}</td><td>{symbol}{c['amount_paid']:,.2f}</td><td>{symbol}{(c['total_fee'] - c['amount_paid']):,.2f}</td><td><a class='btn btn-secondary' href='{url_for('start_paystack_payment', student_id=c['id'])}'>Pay Now</a></td></tr>"
        for c in children
    ) or "<tr><td colspan='7'>No child linked yet.</td></tr>"

    content = f"""
    <h2>My Children</h2>
    <table>
        <tr><th>Student ID</th><th>Name</th><th>Class</th><th>Total Fee</th><th>Paid</th><th>Balance</th><th>Action</th></tr>
        {rows}
    </table>
    """
    return render_page(content)


@app.route('/parent-payments')
@login_required('parent')
def parent_payments():
    conn = get_db_connection()
    symbol = currency_symbol()
    payments = conn.execute(
        """
        SELECT p.*, s.student_code, s.full_name, s.class_name FROM payments p
        JOIN students s ON s.id = p.student_id
        WHERE s.parent_id = ?
        ORDER BY p.id DESC
        """,
        (session['user_id'],),
    ).fetchall()
    conn.close()

    rows = "".join(
        f"<tr><td>{p['student_code'] or '-'}</td><td>{p['full_name']}</td><td>{p['class_name']}</td><td>{symbol}{p['amount_paid']:,.2f}</td><td>{p['term_name']}</td><td>{p['method']}</td><td>{p['status']}</td><td>{p['reference']}</td><td>{p['payment_date'] or '-'}</td><td><a class='btn' href='{url_for('receipt', payment_id=p['id'])}'>Receipt</a></td></tr>"
        for p in payments
    ) or "<tr><td colspan='10'>No payments yet.</td></tr>"

    content = f"""
    <h2>My Payments</h2>
    <table>
        <tr><th>Student ID</th><th>Child</th><th>Class</th><th>Amount</th><th>Term</th><th>Method</th><th>Status</th><th>Reference</th><th>Date</th><th>Receipt</th></tr>
        {rows}
    </table>
    """
    return render_page(content)


@app.route('/start-paystack-payment/<int:student_id>', methods=['GET', 'POST'])
@login_required()
def start_paystack_payment(student_id):
    conn = get_db_connection()
    student = conn.execute("SELECT * FROM students WHERE id = ?", (student_id,)).fetchone()
    if not student:
        conn.close()
        flash('Student not found.')
        return redirect(url_for('students' if session.get('role') == 'admin' else 'parent_dashboard'))

    if session.get('role') == 'parent' and not ensure_parent_can_access_student(conn, session['user_id'], student_id):
        conn.close()
        flash('You cannot pay for that child account.')
        return redirect(url_for('parent_dashboard'))

    balance = get_student_balance(conn, student_id)
    symbol = currency_symbol()

    if request.method == 'POST':
        try:
            if not PAYSTACK_SECRET_KEY:
                raise RuntimeError('PAYSTACK_SECRET_KEY is not configured.')

            amount_paid = float(request.form['amount_paid'])
            term_name = request.form['term_name'].strip()
            note = request.form['note'].strip()

            if amount_paid <= 0:
                raise ValueError('Amount must be greater than zero.')
            if amount_paid > balance:
                raise ValueError("Amount cannot be greater than the student's outstanding balance.")
            if not term_name:
                raise ValueError('Term is required.')
            if not student['parent_email']:
                raise ValueError('Parent email is required for Paystack online payment.')

            reference = generate_reference('PSTK')
            created_by = None if session.get('role') == 'parent' else session.get('user_id')
            conn.execute(
                "INSERT INTO payments (student_id, amount_paid, payment_date, term_name, method, status, reference, note, created_by, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (student_id, amount_paid, None, term_name, 'Online Payment', 'Pending', reference, note, created_by, now_str(), now_str()),
            )
            conn.commit()

            payment_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            paystack_resp = initialize_paystack_transaction(student, amount_paid, term_name, reference, note)
            if not paystack_resp.get('status'):
                raise RuntimeError(paystack_resp.get('message', 'Unable to initialize payment.'))

            data = paystack_resp.get('data', {})
            conn.execute(
                "UPDATE payments SET paystack_reference = ?, paystack_access_code = ?, updated_at = ? WHERE id = ?",
                (data.get('reference'), data.get('access_code'), now_str(), payment_id),
            )
            conn.commit()
            auth_url = data.get('authorization_url')
            conn.close()
            return redirect(auth_url)
        except Exception as exc:
            logger.exception('Paystack initialization failed')
            conn.rollback()
            conn.close()
            flash(f'Could not start online payment: {str(exc)}')
            return redirect(url_for('start_paystack_payment', student_id=student_id))

    conn.close()
    back_url = url_for('students') if session.get('role') == 'admin' else url_for('parent_dashboard')
    content = f"""
    <h2>Start Paystack Online Payment</h2>
    <div class='card'>
        <p><strong>Student ID:</strong> {student['student_code'] or '-'}</p>
        <p><strong>Student:</strong> {student['full_name']}</p>
        <p><strong>Class:</strong> {student['class_name']}</p>
        <p><strong>Parent Email:</strong> {student['parent_email'] or '-'}</p>
        <p><strong>Outstanding Balance:</strong> {symbol}{balance:,.2f}</p>
    </div>
    <form method='post'>
        <div class='row'>
            <div><label>Amount to Pay</label><input type='number' step='0.01' name='amount_paid' max='{balance}' required></div>
            <div><label>Term</label><input type='text' name='term_name' placeholder='e.g. First Term' required></div>
        </div>
        <label>Note</label><input type='text' name='note' placeholder='Optional note'>
        <button type='submit'>Continue to Paystack</button>
        <a href='{back_url}' class='btn btn-warning'>Back</a>
    </form>
    """
    return render_page(content)


@app.route('/paystack/callback')
def paystack_callback():
    reference = request.args.get('reference', '').strip()
    if not reference:
        flash('Missing transaction reference from Paystack callback.')
        return redirect(url_for('login'))

    try:
        verify_resp = verify_paystack_transaction(reference)
        if not verify_resp.get('status'):
            flash('Could not verify the payment with Paystack.')
            return redirect(url_for('login'))

        data = verify_resp.get('data', {})
        conn = get_db_connection()
        payment = conn.execute("SELECT * FROM payments WHERE reference = ?", (reference,)).fetchone()
        if not payment:
            conn.close()
            flash('Local payment record not found for verified transaction.')
            return redirect(url_for('login'))

        ok, message = mark_payment_success(conn, payment, data)
        payment_id = payment['id']
        conn.close()

        if ok:
            flash('Paystack payment verified successfully.')
            return redirect(url_for('receipt', payment_id=payment_id))
        flash(message)
        return redirect(url_for('login'))
    except Exception as exc:
        logger.exception('Paystack callback verification failed')
        flash(f'Payment verification failed: {str(exc)}')
        return redirect(url_for('login'))


@app.route('/paystack/webhook', methods=['POST'])
def paystack_webhook():
    raw_body = request.get_data()
    signature = request.headers.get('x-paystack-signature', '')
    if not signature_is_valid(raw_body, signature):
        logger.warning('Invalid Paystack webhook signature received')
        return jsonify({"ok": False, "message": "Invalid signature"}), 401

    event = request.get_json(silent=True) or {}
    event_type = event.get('event', '')
    data = event.get('data', {})
    event_reference = data.get('reference') or data.get('id') or ''

    conn = get_db_connection()
    inserted = upsert_webhook_event(conn, event_type, str(event_reference), raw_body.decode('utf-8', errors='ignore'))
    if not inserted:
        conn.close()
        return jsonify({"ok": True, "message": "Duplicate event ignored"}), 200

    try:
        if event_type == 'charge.success':
            reference = data.get('reference')
            payment = conn.execute("SELECT * FROM payments WHERE reference = ?", (reference,)).fetchone()
            if payment:
                mark_payment_success(conn, payment, data)
        conn.close()
        return jsonify({"ok": True}), 200
    except Exception:
        conn.close()
        logger.exception('Failed processing Paystack webhook')
        return jsonify({"ok": False, "message": "Processing error"}), 200


@app.route('/receipt/<int:payment_id>')
@login_required()
def receipt(payment_id):
    conn = get_db_connection()
    payment = conn.execute(
        "SELECT p.*, s.student_code, s.full_name, s.class_name, s.parent_phone, s.parent_email, s.total_fee, s.parent_id FROM payments p JOIN students s ON s.id = p.student_id WHERE p.id = ?",
        (payment_id,),
    ).fetchone()

    if not payment:
        conn.close()
        flash('Receipt not found.')
        return redirect(url_for('dashboard' if session.get('role') == 'admin' else 'parent_dashboard'))

    if session.get('role') == 'parent' and payment['parent_id'] != session.get('user_id'):
        conn.close()
        flash('You cannot view that receipt.')
        return redirect(url_for('parent_dashboard'))

    total_paid = conn.execute(
        "SELECT COALESCE(SUM(amount_paid), 0) AS total FROM payments WHERE student_id = ? AND status = 'Paid'",
        (payment['student_id'],),
    ).fetchone()['total']
    balance = payment['total_fee'] - total_paid
    school_name = get_setting(conn, 'school_name', DEFAULT_SCHOOL_NAME)
    school_logo = get_setting(conn, 'school_logo', '')
    conn.close()

    symbol = currency_symbol()
    status_class = 'status-paid' if payment['status'] == 'Paid' else ('status-pending' if payment['status'] == 'Pending' else 'status-failed')

    content = f"""
    <div class='receipt'>
        <div class='top-actions no-print'>
            <a href='{url_for('payments' if session.get('role') == 'admin' else 'parent_payments')}'>Back</a>
            <a href='javascript:window.print()' class='btn btn-secondary'>Print Receipt</a>
        </div>
        <div class='school-brand'>
            {f"<img src='{school_logo}' alt='School Logo'>" if school_logo else ""}
            <div>
                <h2 style='margin-bottom:4px;'>{school_name}</h2>
                <p class='muted' style='margin:0;'>Official School Fee Payment Receipt</p>
            </div>
        </div>
        <p><span class='badge {status_class}'>{payment['status']}</span></p>
        <table>
            <tr><th>Receipt Reference</th><td>{payment['reference']}</td></tr>
            <tr><th>Student ID</th><td>{payment['student_code'] or '-'}</td></tr>
            <tr><th>Student Name</th><td>{payment['full_name']}</td></tr>
            <tr><th>Class</th><td>{payment['class_name']}</td></tr>
            <tr><th>Parent Phone</th><td>{payment['parent_phone'] or '-'}</td></tr>
            <tr><th>Parent Email</th><td>{payment['parent_email'] or '-'}</td></tr>
            <tr><th>Term</th><td>{payment['term_name']}</td></tr>
            <tr><th>Payment Date</th><td>{payment['payment_date'] or '-'}</td></tr>
            <tr><th>Method</th><td>{payment['method']}</td></tr>
            <tr><th>Channel</th><td>{payment['channel'] or '-'}</td></tr>
            <tr><th>Amount Paid</th><td>{symbol}{payment['amount_paid']:,.2f}</td></tr>
            <tr><th>Total School Fee</th><td>{symbol}{payment['total_fee']:,.2f}</td></tr>
            <tr><th>Total Paid So Far</th><td>{symbol}{total_paid:,.2f}</td></tr>
            <tr><th>Outstanding Balance</th><td>{symbol}{balance:,.2f}</td></tr>
            <tr><th>Gateway Response</th><td>{payment['gateway_response'] or '-'}</td></tr>
            <tr><th>Note</th><td>{payment['note'] or '-'}</td></tr>
        </table>
        <p style='margin-top:20px'><strong>Authorized By:</strong> School Accounts Office</p>
    </div>
    """
    return render_page(content)


@app.route('/reports')
@login_required('admin')
def reports():
    conn = get_db_connection()
    symbol = currency_symbol()
    rows_data = conn.execute(
        """
        SELECT s.student_code, s.full_name, s.class_name, s.parent_phone, s.parent_email, s.total_fee,
               COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid
        FROM students s
        LEFT JOIN payments p ON s.id = p.student_id
        GROUP BY s.id
        ORDER BY (s.total_fee - COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0)) DESC
        """
    ).fetchall()
    conn.close()

    rows = "".join(
        f"<tr><td>{r['student_code'] or '-'}</td><td>{r['full_name']}</td><td>{r['class_name']}</td><td>{r['parent_phone'] or '-'}</td><td>{r['parent_email'] or '-'}</td><td>{symbol}{r['total_fee']:,.2f}</td><td>{symbol}{r['amount_paid']:,.2f}</td><td class='danger'>{symbol}{(r['total_fee'] - r['amount_paid']):,.2f}</td></tr>"
        for r in rows_data
    ) or "<tr><td colspan='8'>No report data yet.</td></tr>"

    content = f"""
    <h2>Outstanding Balance Report</h2>
    <table>
        <tr><th>Student ID</th><th>Student</th><th>Class</th><th>Parent Phone</th><th>Parent Email</th><th>Total Fee</th><th>Paid</th><th>Balance</th></tr>
        {rows}
    </table>
    """
    return render_page(content)


@app.route('/settings', methods=['GET', 'POST'])
@login_required('admin')
def settings_page():
    conn = get_db_connection()
    if request.method == 'POST':
        try:
            school_name = request.form['school_name'].strip() or DEFAULT_SCHOOL_NAME
            currency = request.form['currency'].strip() or DEFAULT_CURRENCY
            school_logo_text = request.form['school_logo'].strip()
            uploaded_logo = request.files.get('school_logo_file')
            remove_logo = request.form.get('remove_logo') == 'yes'

            current_logo = get_setting(conn, 'school_logo', '')
            final_logo = current_logo

            if remove_logo:
                final_logo = ''
            elif uploaded_logo and uploaded_logo.filename:
                final_logo = file_to_data_url(uploaded_logo)
            elif school_logo_text:
                final_logo = school_logo_text

            set_setting(conn, 'school_name', school_name)
            set_setting(conn, 'school_logo', final_logo)
            set_setting(conn, 'currency', currency)
            conn.commit()
            conn.close()
            flash('School branding settings updated successfully.')
            return redirect(url_for('settings_page'))
        except Exception as exc:
            conn.close()
            flash(f'Could not update branding settings: {str(exc)}')
            return redirect(url_for('settings_page'))

    current_school_name = get_setting(conn, 'school_name', DEFAULT_SCHOOL_NAME)
    current_school_logo = get_setting(conn, 'school_logo', '')
    current_currency = get_setting(conn, 'currency', DEFAULT_CURRENCY)
    webhook_url = f"{BASE_URL}{url_for('paystack_webhook')}"
    callback_url = f"{BASE_URL}{url_for('paystack_callback')}"
    key_status = 'Configured' if PAYSTACK_SECRET_KEY and PAYSTACK_PUBLIC_KEY else 'Missing keys'
    conn.close()

    logo_preview = f"<img src='{current_school_logo}' alt='School Logo' style='max-width:140px;max-height:140px;border:1px solid #ddd;border-radius:10px;padding:8px;background:#fff;'>" if current_school_logo else "<p class='muted'>No logo uploaded yet.</p>"

    content = f"""
    <h2>School Settings & Branding</h2>
    <form method='post' enctype='multipart/form-data'>
        <div class='row'>
            <div><label>School Name</label><input type='text' name='school_name' value='{current_school_name}' required></div>
            <div><label>Currency Code</label><input type='text' name='currency' value='{current_currency}' placeholder='NGN'></div>
        </div>

        <div class='card' style='margin-bottom:16px;'>
            <h3>Current Logo</h3>
            {logo_preview}
        </div>

        <label>Upload School Logo</label>
        <input type='file' name='school_logo_file' accept='.png,.jpg,.jpeg,.webp,.gif'>
        <p class='muted'>Recommended: square logo, under 2MB.</p>

        <label>Or Paste School Logo URL / Base64 Image</label>
        <textarea name='school_logo' rows='4' placeholder='Paste image URL or data:image/... base64 string'></textarea>

        <div class='row'>
            <div>
                <label>Remove Existing Logo</label>
                <select name='remove_logo'>
                    <option value='no'>No</option>
                    <option value='yes'>Yes</option>
                </select>
            </div>
        </div>

        <button type='submit'>Save Branding</button>
    </form>

    <div class='card'>
        <h3>Paystack Settings</h3>
        <p><strong>Base URL:</strong> {BASE_URL}</p>
        <p><strong>Paystack Keys:</strong> {key_status}</p>
        <p><strong>Callback URL:</strong> <code>{callback_url}</code></p>
        <p><strong>Webhook URL:</strong> <code>{webhook_url}</code></p>
    </div>
    """
    return render_page(content)


@app.route('/export-csv')
@login_required('admin')
def export_csv():
    conn = get_db_connection()
    rows = conn.execute(
        "SELECT s.student_code, s.full_name, s.class_name, s.parent_phone, s.parent_email, s.total_fee, COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid, (s.total_fee - COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0)) AS balance FROM students s LEFT JOIN payments p ON s.id = p.student_id GROUP BY s.id ORDER BY s.full_name ASC"
    ).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Student ID', 'Student Name', 'Class', 'Parent Phone', 'Parent Email', 'Total Fee', 'Amount Paid', 'Balance'])
    for row in rows:
        writer.writerow([row['student_code'], row['full_name'], row['class_name'], row['parent_phone'], row['parent_email'], row['total_fee'], row['amount_paid'], row['balance']])

    mem = io.BytesIO()
    mem.write(output.getvalue().encode('utf-8'))
    mem.seek(0)
    output.close()
    return send_file(mem, mimetype='text/csv', as_attachment=True, download_name='school_fee_report.csv')


@app.route('/search')
@login_required()
def global_search():
    query = request.args.get('q', '').strip()
    if not query:
        flash('Enter something to search.')
        return redirect(url_for('parent_dashboard' if session.get('role') == 'parent' else 'dashboard'))

    conn = get_db_connection()
    symbol = currency_symbol()

    if session.get('role') == 'admin':
        students = conn.execute(
            """
            SELECT s.*, p.full_name AS parent_name,
                   COALESCE(SUM(CASE WHEN py.status = 'Paid' THEN py.amount_paid ELSE 0 END), 0) AS amount_paid
            FROM students s
            LEFT JOIN parents p ON p.id = s.parent_id
            LEFT JOIN payments py ON py.student_id = s.id
            WHERE s.full_name LIKE ? OR s.student_code LIKE ? OR s.class_name LIKE ? OR s.parent_email LIKE ?
            GROUP BY s.id
            ORDER BY s.full_name ASC
            """,
            (f'%{query}%', f'%{query}%', f'%{query}%', f'%{query}%'),
        ).fetchall()
        payments = conn.execute(
            """
            SELECT p.reference, p.amount_paid, p.payment_date, p.status, s.full_name, s.student_code
            FROM payments p
            JOIN students s ON s.id = p.student_id
            WHERE p.reference LIKE ? OR s.student_code LIKE ? OR s.full_name LIKE ?
            ORDER BY p.id DESC
            LIMIT 20
            """,
            (f'%{query}%', f'%{query}%', f'%{query}%'),
        ).fetchall()
    else:
        students = conn.execute(
            """
            SELECT s.*, COALESCE(SUM(CASE WHEN py.status = 'Paid' THEN py.amount_paid ELSE 0 END), 0) AS amount_paid
            FROM students s
            LEFT JOIN payments py ON py.student_id = s.id
            WHERE s.parent_id = ? AND (s.full_name LIKE ? OR s.student_code LIKE ? OR s.class_name LIKE ?)
            GROUP BY s.id
            ORDER BY s.full_name ASC
            """,
            (session.get('user_id'), f'%{query}%', f'%{query}%', f'%{query}%'),
        ).fetchall()
        payments = conn.execute(
            """
            SELECT p.reference, p.amount_paid, p.payment_date, p.status, s.full_name, s.student_code
            FROM payments p
            JOIN students s ON s.id = p.student_id
            WHERE s.parent_id = ? AND (p.reference LIKE ? OR s.student_code LIKE ? OR s.full_name LIKE ?)
            ORDER BY p.id DESC
            LIMIT 20
            """,
            (session.get('user_id'), f'%{query}%', f'%{query}%', f'%{query}%'),
        ).fetchall()

    conn.close()

    student_rows = ''.join(
        f"<tr><td>{s['student_code'] or '-'}</td><td>{s['full_name']}</td><td>{s['class_name']}</td><td>{symbol}{s['total_fee']:,.2f}</td><td>{symbol}{s['amount_paid']:,.2f}</td><td>{symbol}{(s['total_fee'] - s['amount_paid']):,.2f}</td></tr>"
        for s in students
    ) or "<tr><td colspan='6'>No student record found.</td></tr>"

    payment_rows = ''.join(
        f"<tr><td>{p['reference']}</td><td>{p['student_code'] or '-'}</td><td>{p['full_name']}</td><td>{symbol}{p['amount_paid']:,.2f}</td><td>{p['status']}</td><td>{p['payment_date'] or '-'}</td></tr>"
        for p in payments
    ) or "<tr><td colspan='6'>No payment record found.</td></tr>"

    content = f"""
    <div class='hero'>
        <h1>Search Results</h1>
        <p>Showing results for: <strong>{query}</strong></p>
    </div>
    <div class='split'>
        <div>
            <h3>Student Records</h3>
            <table>
                <tr><th>Student ID</th><th>Name</th><th>Class</th><th>Total Fee</th><th>Paid</th><th>Balance</th></tr>
                {student_rows}
            </table>
        </div>
        <div>
            <h3>Payment Records</h3>
            <table>
                <tr><th>Reference</th><th>Student ID</th><th>Name</th><th>Amount</th><th>Status</th><th>Date</th></tr>
                {payment_rows}
            </table>
        </div>
    </div>
    """
    return render_page(content)


@app.route('/health')
def health():
    school_name, _ = school_context()
    return jsonify({"ok": True, "app": school_name, "time": now_str()})


init_db()

if __name__ == '__main__':
    app.run(debug=(APP_ENV == 'development'))
