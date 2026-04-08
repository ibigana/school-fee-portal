from dotenv import load_dotenv
load_dotenv()

import os
import csv
import io
import hmac
import base64
import hashlib
import logging
import uuid
from functools import wraps
from datetime import datetime

import psycopg
from psycopg.rows import dict_row
import requests
from werkzeug.utils import secure_filename
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, session, jsonify

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
header { background: linear-gradient(135deg, #123a5c 0%, #1f4e79 55%, #2c6da3 100%); color: white; padding: 8px 16px; position: sticky; top: 0; z-index: 1000; box-shadow: 0 2px 10px rgba(0,0,0,0.12); }
nav a { color: white; text-decoration: none; font-weight: bold; }
.nav-toggle { display: none; background: transparent; border: 1px solid rgba(255,255,255,0.35); color: white; padding: 8px 12px; border-radius: 8px; font-size: 14px; width: auto; margin: 0; }
.nav-top { display: grid; grid-template-columns: 1fr auto 1fr; align-items: center; gap: 16px; }
.brand-wrap { display: flex; align-items: center; gap: 10px; min-width: 0; }
.brand-wrap h2 { margin: 0; font-size: 1.05rem; overflow-wrap: anywhere; }
.nav-spacer { justify-self: end; }
.nav-menu { display: flex; align-items: center; justify-content: center; gap: 6px; flex-wrap: wrap; margin-top: 6px; }
.nav-menu a { padding: 10px 14px; border-radius: 10px; background: rgba(255,255,255,0.08); }
.container { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
.hero { background: linear-gradient(135deg, #ffffff 0%, #eef5fb 100%); border-radius: 18px; padding: 28px; box-shadow: 0 8px 24px rgba(18,58,92,0.08); margin-bottom: 24px; border: 1px solid rgba(31,78,121,0.08); }
.hero h1 { margin-bottom: 8px; font-size: clamp(1.8rem, 4vw, 2.8rem); }
.hero p { margin: 0; color: #4c5f73; font-size: 1rem; }
.hero-actions { display: flex; gap: 12px; flex-wrap: wrap; margin-top: 18px; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 24px; align-items: stretch; }
.dashboard-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; margin-bottom: 24px; align-items: stretch; }
.card { background: white; border-radius: 16px; padding: 14px 16px; box-shadow: 0 6px 18px rgba(0,0,0,0.06); min-width: 0; overflow: hidden; border: 1px solid rgba(31,78,121,0.07); }
h1, h2, h3, h4 { margin-top: 0; overflow-wrap: anywhere; word-break: break-word; }
.stat-number { font-size: clamp(1.2rem, 2.8vw, 2.2rem); line-height: 1.15; margin: 0; overflow-wrap: anywhere; word-break: break-word; }
.stat-label { margin-bottom: 0; color: #5d6d7e; text-transform: uppercase; letter-spacing: 0.04em; font-size: 0.82rem; }
.stat-header { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; min-width: 0; }
table { width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 10px rgba(0,0,0,0.08); border-radius: 14px; overflow: hidden; }
th, td { padding: 12px; border-bottom: 1px solid #e7ebf0; text-align: left; font-size: 14px; }
th { background: #eaf1f8; color: #284a6b; }
form { background: white; padding: 18px; border-radius: 14px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); margin-bottom: 24px; border: 1px solid rgba(31,78,121,0.07); }
input, select, button, textarea { width: 100%; padding: 12px; margin-top: 6px; margin-bottom: 14px; border: 1px solid #ccd5df; border-radius: 10px; box-sizing: border-box; }
button, .btn { background: linear-gradient(135deg, #1f4e79 0%, #2c6da3 100%); color: white; border: none; cursor: pointer; font-weight: bold; text-decoration: none; display: inline-block; width: auto; padding: 10px 16px; border-radius: 10px; }
.btn-secondary { background: linear-gradient(135deg, #267b42 0%, #35a95a 100%); }
.btn-warning { background: linear-gradient(135deg, #b87500 0%, #de980d 100%); }
.flash { background: #dff0d8; color: #2d572c; padding: 12px; border-radius: 10px; margin-bottom: 16px; border-left: 4px solid #2f7d32; }
.danger { color: #b00020; font-weight: bold; }
.success { color: #2f7d32; font-weight: bold; }
.info { color: #0b5cab; font-weight: bold; }
.muted { color: #666; font-size: 14px; }
.top-actions { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
.auth-box { max-width: 460px; margin: 50px auto; }
.receipt { background: white; padding: 24px; border-radius: 14px; max-width: 820px; margin: 0 auto; }
.badge { display: inline-block; padding: 6px 10px; border-radius: 999px; font-size: 12px; font-weight: bold; background: #eaf1f8; }
.status-paid { background: #dff0d8; color: #2d572c; }
.status-pending { background: #fff3cd; color: #7a5b00; }
.status-failed { background: #f8d7da; color: #842029; }
.school-brand { display: flex; align-items: center; gap: 16px; margin-bottom: 18px; }
.school-brand img { width: 80px; height: 80px; object-fit: contain; border-radius: 8px; background: #fff; border: 1px solid #ddd; }
.mini-logo { width: 42px; height: 42px; object-fit: contain; border-radius: 6px; background: white; }
.split { display: grid; grid-template-columns: 2fr 1fr; gap: 18px; }
.icon-chip { display: inline-flex; align-items: center; justify-content: center; width: 34px; height: 34px; border-radius: 10px; background: linear-gradient(135deg, #eaf1f8 0%, #dbeaf6 100%); font-size: 16px; flex-shrink: 0; }
@media (max-width: 1024px) { .dashboard-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 850px) { .split { grid-template-columns: 1fr; } }
@media (max-width: 768px) { .nav-top { grid-template-columns: 1fr auto; } .nav-spacer { display: none; } .nav-toggle { display: inline-block; } .nav-menu { display: none; width: 100%; flex-direction: column; align-items: stretch; gap: 8px; margin-top: 12px; background: rgba(255,255,255,0.08); padding: 12px; border-radius: 12px; } .nav-menu.show { display: flex; } .dashboard-grid { grid-template-columns: 1fr; } table { display: block; overflow-x: auto; white-space: nowrap; } }
</style>
<script>
function toggleMenu(){ const menu = document.getElementById('mobileNavMenu'); if(menu) menu.classList.toggle('show'); }
</script>
</head>
<body>
{% if request.endpoint == 'landing_page' %}
<div class="container" style="padding-top:32px;">{{ content|safe }}</div>
{% elif session.get('user_id') %}
<header>
    <div style="text-align:center;font-weight:600;margin-bottom:10px;">
        <a href="https://ibigana.com" target="_blank" style="color:white;text-decoration:none;">ibigana.com</a>
    </div>

    <div style="margin-bottom:12px; display:flex; justify-content:flex-end;">
        <form action="{{ url_for('global_search') }}" method="get" style="background:transparent;box-shadow:none;padding:0;margin:0;border:none; width:min(520px, 100%);">
            <div style="display:flex;gap:10px;align-items:center;justify-content:flex-end;">
                <input type="text" name="q" placeholder="Search student by name, student ID, class, parent email, or payment reference" style="flex:1;min-width:240px;margin:0;background:white;" value="{{ request.args.get('q','') if request.endpoint == 'global_search' else '' }}">
                <button type="submit" class="btn" style="margin:0;">Search</button>
            </div>
        </form>
    </div>

    <div class="nav-top">
        <div class="brand-wrap">
            {% if school_logo %}
            <img src="{{ school_logo }}" class="mini-logo" alt="Logo">
            {% endif %}
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
            <a href="{{ url_for('admin_change_password_page') }}">Change Admin Password</a>
        {% else %}
            <a href="{{ url_for('parent_dashboard') }}">My Dashboard</a>
            <a href="{{ url_for('parent_children') }}">My Children</a>
            <a href="{{ url_for('parent_payments') }}">My Payments</a>
        {% endif %}
        <a href="{{ url_for('logout') }}">Logout</a>
    </nav>
</header>
<div class="container">{% with messages = get_flashed_messages() %}{% if messages %}{% for message in messages %}<div class="flash">{{ message }}</div>{% endfor %}{% endif %}{% endwith %}{{ content|safe }}</div>
{% else %}
<div class="container">{% with messages = get_flashed_messages() %}{% if messages %}{% for message in messages %}<div class="flash">{{ message }}</div>{% endfor %}{% endif %}{% endwith %}{{ content|safe }}</div>
{% endif %}
</body>
</html>
"""

def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing. Set it in your environment variables.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def db_execute(query, params=(), fetchone=False, fetchall=False, commit=False):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            result = None
            if fetchone:
                result = cur.fetchone()
            elif fetchall:
                result = cur.fetchall()
            if commit:
                conn.commit()
            return result

def hash_password(password): return hashlib.sha256(password.encode()).hexdigest()
def generate_reference(prefix="TXN"): return f"{prefix}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
def now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def generate_student_code():
    year_part = datetime.now().strftime('%Y')
    row = db_execute("SELECT student_code FROM students WHERE student_code LIKE %s ORDER BY id DESC LIMIT 1", (f"STD-{year_part}-%",), fetchone=True)
    next_no = 1
    if row and row["student_code"]:
        try:
            next_no = int(str(row["student_code"]).split("-")[-1]) + 1
        except Exception:
            next_no = 1
    return f"STD-{year_part}-{next_no:05d}"

def get_setting(key, default=""):
    row = db_execute("SELECT value FROM settings WHERE key_name = %s", (key,), fetchone=True)
    return row["value"] if row else default

def set_setting(key, value):
    row = db_execute("SELECT id FROM settings WHERE key_name = %s", (key,), fetchone=True)
    if row:
        db_execute("UPDATE settings SET value = %s WHERE key_name = %s", (value, key), commit=True)
    else:
        db_execute("INSERT INTO settings (key_name, value) VALUES (%s, %s)", (key, value), commit=True)

def school_context(): return get_setting("school_name", DEFAULT_SCHOOL_NAME), get_setting("school_logo", "")
def render_page(content):
    school_name, school_logo = school_context()
    return render_template_string(BASE_HTML, content=content, school_name=school_name, school_logo=school_logo)
def currency_symbol(): return "₦" if get_setting("currency", DEFAULT_CURRENCY) == "NGN" else get_setting("currency", DEFAULT_CURRENCY) + " "

def current_user():
    if session.get("user_id"):
        return {"id": session.get("user_id"), "username": session.get("username"), "role": session.get("role")}
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
                return redirect(url_for("parent_dashboard") if user.get("role") == "parent" else url_for("dashboard"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def balance_breakdown(total_fee, amount_paid):
    balance = float(total_fee) - float(amount_paid)
    if balance > 0:
        return {"status": "Owing School", "amount_due": balance, "credit_balance": 0.0, "status_class": "danger"}
    if balance < 0:
        return {"status": "School Owes Student", "amount_due": 0.0, "credit_balance": abs(balance), "status_class": "info"}
    return {"status": "Balanced", "amount_due": 0.0, "credit_balance": 0.0, "status_class": "success"}

def get_student_balance(student_id):
    row = db_execute("""
        SELECT s.total_fee, COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid
        FROM students s LEFT JOIN payments p ON p.student_id = s.id WHERE s.id = %s GROUP BY s.id
    """, (student_id,), fetchone=True)
    if not row:
        return 0.0
    return float(row["total_fee"] - row["amount_paid"])

def parent_total_balance(parent_id):
    rows = db_execute("SELECT id FROM students WHERE parent_id = %s", (parent_id,), fetchall=True) or []
    return sum(get_student_balance(r["id"]) for r in rows)

def paystack_headers():
    if not PAYSTACK_SECRET_KEY:
        raise RuntimeError("PAYSTACK_SECRET_KEY is missing.")
    return {"Authorization": f"Bearer {PAYSTACK_SECRET_KEY}", "Content-Type": "application/json"}

def safe_amount_to_kobo(amount_naira): return int(round(float(amount_naira) * 100))

def initialize_paystack_transaction(student, amount_paid, term_name, local_reference, note=""):
    payload = {"email": student["parent_email"] or f"student-{student['id']}@example.com", "amount": safe_amount_to_kobo(amount_paid), "currency": DEFAULT_CURRENCY, "reference": local_reference, "callback_url": f"{BASE_URL}{url_for('paystack_callback')}", "metadata": {"student_id": student["id"], "student_name": student["full_name"], "class_name": student["class_name"], "term_name": term_name, "local_reference": local_reference, "note": note}}
    response = requests.post(f"{PAYSTACK_API_BASE}/transaction/initialize", headers=paystack_headers(), json=payload, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()

def verify_paystack_transaction(reference):
    response = requests.get(f"{PAYSTACK_API_BASE}/transaction/verify/{reference}", headers=paystack_headers(), timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()

def signature_is_valid(raw_body, signature):
    if not PAYSTACK_SECRET_KEY or not signature:
        return False
    computed = hmac.new(PAYSTACK_SECRET_KEY.encode("utf-8"), raw_body, hashlib.sha512).hexdigest()
    return hmac.compare_digest(computed, signature)

def mark_payment_success(payment_row, verified_data):
    status = verified_data.get("status")
    gateway_response = verified_data.get("gateway_response") or verified_data.get("message") or ""
    channel = verified_data.get("channel") or ""
    paid_at = verified_data.get("paid_at") or now_str()
    amount_kobo = verified_data.get("amount", 0)
    expected_kobo = safe_amount_to_kobo(payment_row["amount_paid"])
    if status != "success":
        db_execute("UPDATE payments SET status = %s, gateway_response = %s, channel = %s, updated_at = %s WHERE id = %s", ("Failed", gateway_response, channel, now_str(), payment_row["id"]), commit=True)
        return False, "Verification returned non-success status."
    if int(amount_kobo) != int(expected_kobo):
        db_execute("UPDATE payments SET status = %s, gateway_response = %s, channel = %s, updated_at = %s WHERE id = %s", ("Failed", "Amount mismatch", channel, now_str(), payment_row["id"]), commit=True)
        return False, "Amount mismatch during verification."
    db_execute("UPDATE payments SET status = %s, payment_date = %s, paid_at = %s, gateway_response = %s, channel = %s, updated_at = %s WHERE id = %s", ("Paid", paid_at[:10], paid_at, gateway_response, channel, now_str(), payment_row["id"]), commit=True)
    return True, "Payment verified successfully."

def upsert_webhook_event(event_type, event_reference, payload_text):
    exists = db_execute("SELECT id FROM webhook_events WHERE event_type = %s AND event_reference = %s", (event_type, event_reference), fetchone=True)
    if exists:
        return False
    db_execute("INSERT INTO webhook_events (event_type, event_reference, payload, received_at) VALUES (%s, %s, %s, %s)", (event_type, event_reference, payload_text, now_str()), commit=True)
    return True

def ensure_parent_can_access_student(parent_id, student_id):
    return db_execute("SELECT id FROM students WHERE id = %s AND parent_id = %s", (student_id, parent_id), fetchone=True) is not None

def allowed_logo_file(filename): return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS

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
    mime_map = {"png":"image/png","jpg":"image/jpeg","jpeg":"image/jpeg","webp":"image/webp","gif":"image/gif"}
    encoded = base64.b64encode(file_bytes).decode("utf-8")
    return f"data:{mime_map.get(extension, 'image/png')};base64,{encoded}"

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, full_name TEXT NOT NULL, username TEXT NOT NULL UNIQUE, password_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'admin', created_at TEXT NOT NULL)")
            cur.execute("CREATE TABLE IF NOT EXISTS parents (id SERIAL PRIMARY KEY, full_name TEXT NOT NULL, email TEXT NOT NULL UNIQUE, phone TEXT, password_hash TEXT NOT NULL, created_at TEXT NOT NULL)")
            cur.execute("CREATE TABLE IF NOT EXISTS students (id SERIAL PRIMARY KEY, student_code TEXT UNIQUE, full_name TEXT NOT NULL, class_name TEXT NOT NULL, parent_phone TEXT, parent_email TEXT, parent_id INTEGER, total_fee DOUBLE PRECISION NOT NULL DEFAULT 0, created_at TEXT NOT NULL)")
            cur.execute("CREATE TABLE IF NOT EXISTS payments (id SERIAL PRIMARY KEY, student_id INTEGER NOT NULL, amount_paid DOUBLE PRECISION NOT NULL, payment_date TEXT, term_name TEXT NOT NULL, method TEXT NOT NULL DEFAULT 'Cash', status TEXT NOT NULL DEFAULT 'Pending', reference TEXT NOT NULL UNIQUE, note TEXT, gateway_response TEXT, paystack_reference TEXT, paystack_access_code TEXT, channel TEXT, paid_at TEXT, created_by INTEGER, created_at TEXT NOT NULL, updated_at TEXT)")
            cur.execute("CREATE TABLE IF NOT EXISTS webhook_events (id SERIAL PRIMARY KEY, event_type TEXT NOT NULL, event_reference TEXT, payload TEXT NOT NULL, received_at TEXT NOT NULL, UNIQUE(event_type, event_reference))")
            cur.execute("CREATE TABLE IF NOT EXISTS settings (id SERIAL PRIMARY KEY, key_name TEXT NOT NULL UNIQUE, value TEXT)")
            conn.commit()
    if not db_execute("SELECT id FROM users WHERE username = %s", ("admin",), fetchone=True):
        db_execute("INSERT INTO users (full_name, username, password_hash, role, created_at) VALUES (%s, %s, %s, %s, %s)", ("System Administrator","admin",hash_password("admin123"),"admin",now_str()), commit=True)
    rows = db_execute("SELECT id FROM students WHERE student_code IS NULL OR student_code = ''", fetchall=True) or []
    for row in rows:
        db_execute("UPDATE students SET student_code = %s WHERE id = %s", (generate_student_code(), row["id"]), commit=True)
    set_setting("school_name", get_setting("school_name", DEFAULT_SCHOOL_NAME))
    set_setting("school_logo", get_setting("school_logo", ""))
    set_setting("currency", get_setting("currency", DEFAULT_CURRENCY))

@app.route('/')
def landing_page():
    if current_user():
        return redirect(url_for('parent_dashboard') if session.get('role') == 'parent' else url_for('dashboard'))
    school_name, _ = school_context()
    return render_page(f"<div class='hero'><h1>{school_name}</h1><p>Welcome to the digital school portal for parent access, student dashboards, receipts, and secure online fee payments.</p><div class='hero-actions'><a href='{url_for('login')}' class='btn'>Parent / Admin Login</a><a href='{url_for('health')}' class='btn btn-secondary'>Portal Status</a></div></div>")

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user():
        return redirect(url_for('parent_dashboard') if session.get('role') == 'parent' else url_for('dashboard'))
    if request.method == 'POST':
        user_type = request.form['user_type'].strip()
        username = request.form['username'].strip()
        password = request.form['password'].strip()
        if user_type == 'admin':
            user = db_execute("SELECT * FROM users WHERE username = %s AND password_hash = %s", (username, hash_password(password)), fetchone=True)
            if user:
                session['user_id'], session['username'], session['role'] = user['id'], user['username'], 'admin'
                flash('Admin login successful.')
                return redirect(url_for('dashboard'))
        else:
            parent = db_execute("SELECT * FROM parents WHERE email = %s AND password_hash = %s", (username, hash_password(password)), fetchone=True)
            if parent:
                session['user_id'], session['username'], session['role'] = parent['id'], parent['email'], 'parent'
                flash('Parent login successful.')
                return redirect(url_for('parent_dashboard'))
        flash('Invalid login details.')
        return redirect(url_for('login'))
    return render_page("<div class='auth-box'><form method='post'><h2>Login</h2><label>User Type</label><select name='user_type'><option value='admin'>School Admin</option><option value='parent'>Parent</option></select><label>Username / Email</label><input name='username' required><label>Password</label><input type='password' name='password' required><button type='submit'>Login</button><p class='muted'>Default admin login: <strong>admin</strong> / <strong>admin123</strong></p></form></div>")

@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.')
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required('admin')
def dashboard():
    total_students = db_execute("SELECT COUNT(*) AS count FROM students", fetchone=True)['count']
    total_parents = db_execute("SELECT COUNT(*) AS count FROM parents", fetchone=True)['count']
    total_expected = db_execute("SELECT COALESCE(SUM(total_fee),0) AS total FROM students", fetchone=True)['total']
    total_received = db_execute("SELECT COALESCE(SUM(amount_paid),0) AS total FROM payments WHERE status = %s", ('Paid',), fetchone=True)['total']
    total_pending = db_execute("SELECT COALESCE(SUM(amount_paid),0) AS total FROM payments WHERE status = %s", ('Pending',), fetchone=True)['total']
    balance_rows = db_execute("SELECT s.total_fee, COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid FROM students s LEFT JOIN payments p ON p.student_id = s.id GROUP BY s.id", fetchall=True) or []
    owing_count = balanced_count = credit_count = 0
    owing_total = 0.0
    credit_total = 0.0
    for r in balance_rows:
        b = balance_breakdown(r['total_fee'], r['amount_paid'])
        if b['status'] == 'Owing School':
            owing_count += 1
            owing_total += b['amount_due']
        elif b['status'] == 'School Owes Student':
            credit_count += 1
            credit_total += b['credit_balance']
        else:
            balanced_count += 1
    symbol = currency_symbol()
    recent = db_execute("SELECT p.id, s.student_code, s.full_name, p.amount_paid, p.payment_date, p.term_name, p.reference, p.status, p.method, p.channel FROM payments p JOIN students s ON s.id = p.student_id ORDER BY p.id DESC LIMIT 10", fetchall=True) or []
    rows = "".join(f"<tr><td>{p['student_code'] or '-'}</td><td>{p['full_name']}</td><td>{symbol}{p['amount_paid']:,.2f}</td><td>{p['term_name']}</td><td>{p['method']}</td><td>{p['channel'] or '-'}</td><td>{p['status']}</td><td>{p['reference']}</td><td>{p['payment_date'] or '-'}</td></tr>" for p in recent) or "<tr><td colspan='9'>No payments yet.</td></tr>"
    content = f"<div class='hero'><h1>School Finance Control Center</h1><p>Manage parents, students, receipts, and online school fee payments from one professional dashboard.</p></div><div class='dashboard-grid'><div class='card'><div class='stat-header'><div class='icon-chip'>🎓</div><h3 class='stat-label'>Total Students</h3></div><h1 class='stat-number'>{total_students}</h1></div><div class='card'><div class='stat-header'><div class='icon-chip'>👨‍👩‍👧</div><h3 class='stat-label'>Total Parents</h3></div><h1 class='stat-number'>{total_parents}</h1></div><div class='card'><div class='stat-header'><div class='icon-chip'>💼</div><h3 class='stat-label'>Total Expected Fees</h3></div><h1 class='stat-number'>{symbol}{total_expected:,.2f}</h1></div><div class='card'><div class='stat-header'><div class='icon-chip'>✅</div><h3 class='stat-label'>Total Received</h3></div><h1 class='stat-number success'>{symbol}{total_received:,.2f}</h1></div><div class='card'><div class='stat-header'><div class='icon-chip'>⚠️</div><h3 class='stat-label'>Students Owing</h3></div><h1 class='stat-number danger'>{owing_count}</h1><p class='muted'>{symbol}{owing_total:,.2f}</p></div><div class='card'><div class='stat-header'><div class='icon-chip'>💳</div><h3 class='stat-label'>Credit Balance</h3></div><h1 class='stat-number info'>{credit_count}</h1><p class='muted'>{symbol}{credit_total:,.2f}</p></div></div><div class='card' style='margin-bottom:18px;'><strong>Balanced Students:</strong> <span class='success'>{balanced_count}</span> &nbsp; | &nbsp; <strong>Pending Transactions:</strong> {symbol}{total_pending:,.2f}</div><table><tr><th>Student ID</th><th>Student</th><th>Amount</th><th>Term</th><th>Method</th><th>Channel</th><th>Status</th><th>Reference</th><th>Date</th></tr>{rows}</table>"
    return render_page(content)

@app.route('/admin-accounts', methods=['GET', 'POST'])
@login_required('admin')
def admin_accounts_page():
    if request.method == 'POST':
        try:
            db_execute("INSERT INTO users (full_name, username, password_hash, role, created_at) VALUES (%s, %s, %s, %s, %s)", (request.form['full_name'].strip(), request.form['username'].strip(), hash_password(request.form['password'].strip()), 'admin', now_str()), commit=True)
            flash('Admin account created successfully.')
        except Exception:
            flash('That username already exists.')
        return redirect(url_for('admin_accounts_page'))
    admins = db_execute("SELECT full_name, username, role, created_at FROM users WHERE role = %s ORDER BY id DESC", ('admin',), fetchall=True) or []
    rows = "".join(f"<tr><td>{a['full_name']}</td><td>{a['username']}</td><td>{a['role']}</td><td>{a['created_at']}</td></tr>" for a in admins) or "<tr><td colspan='4'>No admin accounts found.</td></tr>"
    return render_page(f"<form method='post'><div class='grid'><div><label>Full Name</label><input name='full_name' required></div><div><label>Username</label><input name='username' required></div><div><label>Password</label><input name='password' required></div></div><button type='submit'>Create Admin</button></form><table><tr><th>Full Name</th><th>Username</th><th>Role</th><th>Created At</th></tr>{rows}</table>")

@app.route('/admin-change-password', methods=['GET', 'POST'])
@login_required('admin')
def admin_change_password_page():
    if request.method == 'POST':
        db_execute("UPDATE users SET password_hash = %s WHERE username = %s AND role = %s", (hash_password(request.form['new_password'].strip()), request.form['username'].strip(), 'admin'), commit=True)
        flash('Admin password changed successfully.')
        return redirect(url_for('admin_change_password_page'))
    admins = db_execute("SELECT full_name, username, created_at FROM users WHERE role = %s ORDER BY id DESC", ('admin',), fetchall=True) or []
    options = ''.join(f"<option value='{a['username']}'>{a['full_name']} - {a['username']}</option>" for a in admins)
    rows = ''.join(f"<tr><td>{a['full_name']}</td><td>{a['username']}</td><td>{a['created_at']}</td></tr>" for a in admins)
    return render_page(f"<form method='post'><div class='grid'><div><label>Select Admin</label><select name='username'>{options}</select></div><div><label>New Password</label><input name='new_password' required></div></div><button type='submit'>Change Password</button></form><table><tr><th>Full Name</th><th>Username</th><th>Created At</th></tr>{rows}</table>")

@app.route('/parents', methods=['GET', 'POST'])
@login_required('admin')
def parents_page():
    if request.method == 'POST':
        try:
            db_execute("INSERT INTO parents (full_name, email, phone, password_hash, created_at) VALUES (%s, %s, %s, %s, %s)", (request.form['full_name'].strip(), request.form['email'].strip().lower(), request.form['phone'].strip(), hash_password(request.form['password'].strip()), now_str()), commit=True)
            flash('Parent account created successfully.')
        except Exception:
            flash('That parent email already exists.')
        return redirect(url_for('parents_page'))
    parents = db_execute("SELECT p.*, (SELECT COUNT(*) FROM students s WHERE s.parent_id = p.id) AS child_count FROM parents p ORDER BY p.id DESC", fetchall=True) or []
    rows = "".join(f"<tr><td>{p['full_name']}</td><td>{p['email']}</td><td>{p['phone'] or '-'}</td><td>{p['child_count']}</td></tr>" for p in parents) or "<tr><td colspan='4'>No parent accounts yet.</td></tr>"
    return render_page(f"<form method='post'><div class='grid'><div><label>Full Name</label><input name='full_name' required></div><div><label>Email</label><input name='email' required></div><div><label>Phone</label><input name='phone'></div><div><label>Password</label><input name='password' required></div></div><button type='submit'>Save Parent Account</button></form><table><tr><th>Name</th><th>Email</th><th>Phone</th><th>Children</th></tr>{rows}</table>")

@app.route('/students', methods=['GET', 'POST'])
@login_required('admin')
def students():
    symbol = currency_symbol()
    if request.method == 'POST':
        parent_id = request.form['parent_id'].strip()
        parent_id_value = int(parent_id) if parent_id else None
        db_execute("INSERT INTO students (student_code, full_name, class_name, parent_phone, parent_email, parent_id, total_fee, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (generate_student_code(), request.form['full_name'].strip(), request.form['class_name'].strip(), request.form['parent_phone'].strip(), request.form['parent_email'].strip().lower(), parent_id_value, float(request.form['total_fee'].strip() or 0), now_str()), commit=True)
        flash('Student added successfully.')
        return redirect(url_for('students'))
    parent_options = db_execute("SELECT id, full_name, email FROM parents ORDER BY full_name ASC", fetchall=True) or []
    student_rows = db_execute("""
        SELECT s.*, p.full_name AS parent_name, COALESCE(SUM(CASE WHEN py.status = 'Paid' THEN py.amount_paid ELSE 0 END), 0) AS amount_paid
        FROM students s LEFT JOIN parents p ON p.id = s.parent_id LEFT JOIN payments py ON s.id = py.student_id
        GROUP BY s.id, p.full_name ORDER BY s.id DESC
    """, fetchall=True) or []
    options = "".join(f"<option value='{p['id']}'>{p['full_name']} - {p['email']}</option>" for p in parent_options)
    rows = ""
    for s in student_rows:
        b = balance_breakdown(s['total_fee'], s['amount_paid'])
        rows += f"<tr><td>{s['student_code'] or '-'}</td><td>{s['full_name']}</td><td>{s['class_name']}</td><td>{s['parent_name'] or '-'}</td><td>{s['parent_email'] or '-'}</td><td>{symbol}{s['total_fee']:,.2f}</td><td>{symbol}{s['amount_paid']:,.2f}</td><td>{symbol}{b['amount_due']:,.2f}</td><td>{symbol}{b['credit_balance']:,.2f}</td><td class='{b['status_class']}'>{b['status']}</td><td><a class='btn btn-secondary' href='{url_for('start_paystack_payment', student_id=s['id'])}'>Pay Online</a></td></tr>"
    rows = rows or "<tr><td colspan='11'>No students yet.</td></tr>"
    return render_page(f"<form method='post'><div class='grid'><div><label>Full Name</label><input name='full_name' required></div><div><label>Class</label><input name='class_name' required></div><div><label>Link Parent Account</label><select name='parent_id'><option value=''>Select Parent</option>{options}</select></div><div><label>Parent Email</label><input name='parent_email'></div><div><label>Parent Phone</label><input name='parent_phone'></div><div><label>Total Fee</label><input name='total_fee' required></div></div><button type='submit'>Save Student</button></form><table><tr><th>Student ID</th><th>Name</th><th>Class</th><th>Parent</th><th>Parent Email</th><th>Total Fee</th><th>Paid</th><th>Amount Due</th><th>Credit Balance</th><th>Status</th><th>Online Payment</th></tr>{rows}</table>")

@app.route('/payments', methods=['GET', 'POST'])
@login_required('admin')
def payments():
    symbol = currency_symbol()
    if request.method == 'POST':
        payment_id = db_execute("INSERT INTO payments (student_id, amount_paid, payment_date, term_name, method, status, reference, note, created_by, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (int(request.form['student_id']), float(request.form['amount_paid']), request.form['payment_date'], request.form['term_name'].strip(), request.form['method'].strip(), 'Paid', generate_reference('RCP'), request.form['note'].strip(), session.get('user_id'), now_str(), now_str()), fetchone=True, commit=True)["id"]
        flash('Manual payment recorded successfully.')
        return redirect(url_for('receipt', payment_id=payment_id))
    student_options = db_execute("SELECT id, student_code, full_name, class_name FROM students ORDER BY full_name ASC", fetchall=True) or []
    payment_rows = db_execute("SELECT p.id, s.student_code, s.full_name, s.class_name, p.amount_paid, p.payment_date, p.term_name, p.reference, p.method, p.status FROM payments p JOIN students s ON s.id = p.student_id ORDER BY p.id DESC", fetchall=True) or []
    options = "".join(f"<option value='{s['id']}'>{s['student_code'] or '-'} - {s['full_name']} - {s['class_name']}</option>" for s in student_options)
    rows = "".join(f"<tr><td>{p['student_code'] or '-'}</td><td>{p['full_name']}</td><td>{p['class_name']}</td><td>{symbol}{p['amount_paid']:,.2f}</td><td>{p['term_name']}</td><td>{p['method']}</td><td>{p['status']}</td><td>{p['reference']}</td><td>{p['payment_date'] or '-'}</td><td><a class='btn' href='{url_for('receipt', payment_id=p['id'])}'>Receipt</a></td></tr>" for p in payment_rows) or "<tr><td colspan='10'>No payments yet.</td></tr>"
    return render_page(f"<form method='post'><div class='grid'><div><label>Student</label><select name='student_id'>{options}</select></div><div><label>Amount Paid</label><input name='amount_paid' required></div><div><label>Payment Date</label><input type='date' name='payment_date' value='{datetime.now().strftime("%Y-%m-%d")}' required></div><div><label>Term</label><input name='term_name' required></div><div><label>Method</label><select name='method'><option value='Cash'>Cash</option><option value='Bank Transfer'>Bank Transfer</option><option value='POS'>POS</option></select></div><div><label>Note</label><input name='note'></div></div><button type='submit'>Save Manual Payment</button></form><table><tr><th>Student ID</th><th>Student</th><th>Class</th><th>Amount</th><th>Term</th><th>Method</th><th>Status</th><th>Reference</th><th>Date</th><th>Receipt</th></tr>{rows}</table>")

@app.route('/parent-dashboard')
@login_required('parent')
def parent_dashboard():
    parent = db_execute("SELECT * FROM parents WHERE id = %s", (session['user_id'],), fetchone=True)
    children = db_execute("""
        SELECT s.*, COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid
        FROM students s LEFT JOIN payments p ON p.student_id = s.id WHERE s.parent_id = %s GROUP BY s.id ORDER BY s.full_name ASC
    """, (session['user_id'],), fetchall=True) or []
    symbol = currency_symbol()
    total_balance = parent_total_balance(session['user_id'])
    child_rows = ""
    for c in children:
        b = balance_breakdown(c['total_fee'], c['amount_paid'])
        child_rows += f"<tr><td>{c['student_code'] or '-'}</td><td>{c['full_name']}</td><td>{c['class_name']}</td><td>{symbol}{c['total_fee']:,.2f}</td><td>{symbol}{c['amount_paid']:,.2f}</td><td>{symbol}{b['amount_due']:,.2f}</td><td>{symbol}{b['credit_balance']:,.2f}</td><td class='{b['status_class']}'>{b['status']}</td><td><a class='btn btn-secondary' href='{url_for('start_paystack_payment', student_id=c['id'])}'>Pay Now</a></td></tr>"
    child_rows = child_rows or "<tr><td colspan='9'>No child linked yet.</td></tr>"
    return render_page(f"<div class='grid'><div class='card'><div class='stat-header'><div class='icon-chip'>👤</div><h3 class='stat-label'>Parent</h3></div><h2 class='stat-number'>{parent['full_name']}</h2><p>{parent['email']}</p></div><div class='card'><div class='stat-header'><div class='icon-chip'>👶</div><h3 class='stat-label'>Children</h3></div><h1 class='stat-number'>{len(children)}</h1></div><div class='card'><div class='stat-header'><div class='icon-chip'>📌</div><h3 class='stat-label'>Net Balance</h3></div><h1 class='stat-number'>{symbol}{total_balance:,.2f}</h1></div></div><table><tr><th>Student ID</th><th>Name</th><th>Class</th><th>Total Fee</th><th>Paid</th><th>Amount Due</th><th>Credit Balance</th><th>Status</th><th>Action</th></tr>{child_rows}</table>")

@app.route('/parent-children')
@login_required('parent')
def parent_children():
    return redirect(url_for('parent_dashboard'))

@app.route('/parent-payments')
@login_required('parent')
def parent_payments():
    symbol = currency_symbol()
    payments = db_execute("SELECT p.*, s.student_code, s.full_name, s.class_name FROM payments p JOIN students s ON s.id = p.student_id WHERE s.parent_id = %s ORDER BY p.id DESC", (session['user_id'],), fetchall=True) or []
    rows = "".join(f"<tr><td>{p['student_code'] or '-'}</td><td>{p['full_name']}</td><td>{p['class_name']}</td><td>{symbol}{p['amount_paid']:,.2f}</td><td>{p['term_name']}</td><td>{p['method']}</td><td>{p['status']}</td><td>{p['reference']}</td><td>{p['payment_date'] or '-'}</td></tr>" for p in payments) or "<tr><td colspan='9'>No payments yet.</td></tr>"
    return render_page(f"<table><tr><th>Student ID</th><th>Child</th><th>Class</th><th>Amount</th><th>Term</th><th>Method</th><th>Status</th><th>Reference</th><th>Date</th></tr>{rows}</table>")

@app.route('/start-paystack-payment/<int:student_id>', methods=['GET', 'POST'])
@login_required()
def start_paystack_payment(student_id):
    student = db_execute("SELECT * FROM students WHERE id = %s", (student_id,), fetchone=True)
    if not student:
        flash('Student not found.')
        return redirect(url_for('dashboard'))
    if session.get('role') == 'parent' and not ensure_parent_can_access_student(session['user_id'], student_id):
        flash('You cannot pay for that child account.')
        return redirect(url_for('parent_dashboard'))
    balance = max(get_student_balance(student_id), 0)
    symbol = currency_symbol()
    if request.method == 'POST':
        try:
            amount_paid = float(request.form['amount_paid'])
            term_name = request.form['term_name'].strip()
            note = request.form['note'].strip()
            ref = generate_reference('PSTK')
            payment_id = db_execute("INSERT INTO payments (student_id, amount_paid, payment_date, term_name, method, status, reference, note, created_by, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id", (student_id, amount_paid, None, term_name, 'Online Payment', 'Pending', ref, note, None if session.get('role') == 'parent' else session.get('user_id'), now_str(), now_str()), fetchone=True, commit=True)["id"]
            paystack_resp = initialize_paystack_transaction(student, amount_paid, term_name, ref, note)
            data = paystack_resp.get('data', {})
            db_execute("UPDATE payments SET paystack_reference = %s, paystack_access_code = %s, updated_at = %s WHERE id = %s", (data.get('reference'), data.get('access_code'), now_str(), payment_id), commit=True)
            return redirect(data.get('authorization_url'))
        except Exception as exc:
            flash(f'Could not start online payment: {exc}')
    return render_page(f"<div class='card'><p><strong>Student ID:</strong> {student['student_code'] or '-'}</p><p><strong>Student:</strong> {student['full_name']}</p><p><strong>Current Amount Due:</strong> {symbol}{balance:,.2f}</p></div><form method='post'><div class='grid'><div><label>Amount to Pay</label><input name='amount_paid' required></div><div><label>Term</label><input name='term_name' required></div><div><label>Note</label><input name='note'></div></div><button type='submit'>Continue to Paystack</button></form>")

@app.route('/paystack/callback')
def paystack_callback():
    reference = request.args.get('reference', '').strip()
    if not reference:
        flash('Missing transaction reference from Paystack callback.')
        return redirect(url_for('login'))
    verify_resp = verify_paystack_transaction(reference)
    payment = db_execute("SELECT * FROM payments WHERE reference = %s", (reference,), fetchone=True)
    if payment:
        ok, message = mark_payment_success(payment, verify_resp.get('data', {}))
        flash('Paystack payment verified successfully.' if ok else message)
        return redirect(url_for('receipt', payment_id=payment['id']))
    flash('Payment record not found.')
    return redirect(url_for('login'))

@app.route('/paystack/webhook', methods=['POST'])
def paystack_webhook():
    raw_body = request.get_data()
    signature = request.headers.get('x-paystack-signature', '')
    if not signature_is_valid(raw_body, signature):
        return jsonify({"ok": False, "message": "Invalid signature"}), 401
    event = request.get_json(silent=True) or {}
    data = event.get('data', {})
    if not upsert_webhook_event(event.get('event', ''), str(data.get('reference') or data.get('id') or ''), raw_body.decode('utf-8', errors='ignore')):
        return jsonify({"ok": True, "message": "Duplicate event ignored"}), 200
    if event.get('event') == 'charge.success':
        payment = db_execute("SELECT * FROM payments WHERE reference = %s", (data.get('reference'),), fetchone=True)
        if payment:
            mark_payment_success(payment, data)
    return jsonify({"ok": True}), 200

@app.route('/receipt/<int:payment_id>')
@login_required()
def receipt(payment_id):
    payment = db_execute("SELECT p.*, s.student_code, s.full_name, s.class_name, s.parent_phone, s.parent_email, s.total_fee, s.parent_id FROM payments p JOIN students s ON s.id = p.student_id WHERE p.id = %s", (payment_id,), fetchone=True)
    if not payment:
        flash('Receipt not found.')
        return redirect(url_for('dashboard'))

    total_paid = db_execute("SELECT COALESCE(SUM(amount_paid),0) AS total FROM payments WHERE student_id = %s AND status = %s", (payment['student_id'], 'Paid'), fetchone=True)['total']
    b = balance_breakdown(payment['total_fee'], total_paid)
    symbol = currency_symbol()
    school_name, school_logo = school_context()
    status_class = 'status-paid' if payment['status'] == 'Paid' else ('status-pending' if payment['status'] == 'Pending' else 'status-failed')
    logo_html = f"<img src='{school_logo}' alt='School Logo'>" if school_logo else ''

    content = f"""
    <div class='top-actions'>
        <a href='{url_for('payments' if session.get('role') == 'admin' else 'parent_payments')}' class='btn btn-warning'>Return to Payments</a>
        <a href='javascript:window.print()' class='btn btn-secondary'>Print Receipt</a>
    </div>
    <div class='receipt'>
        <div class='school-brand'>
            {logo_html}
            <div><h2>{school_name}</h2></div>
        </div>
        <p><span class='badge {status_class}'>{payment['status']}</span></p>
        <table>
            <tr><th>Receipt Reference</th><td>{payment['reference']}</td></tr>
            <tr><th>Student ID</th><td>{payment['student_code'] or '-'}</td></tr>
            <tr><th>Student Name</th><td>{payment['full_name']}</td></tr>
            <tr><th>Class</th><td>{payment['class_name']}</td></tr>
            <tr><th>Amount Paid</th><td>{symbol}{payment['amount_paid']:,.2f}</td></tr>
            <tr><th>Total School Fee</th><td>{symbol}{payment['total_fee']:,.2f}</td></tr>
            <tr><th>Total Paid So Far</th><td>{symbol}{total_paid:,.2f}</td></tr>
            <tr><th>Amount Due</th><td>{symbol}{b['amount_due']:,.2f}</td></tr>
            <tr><th>Credit Balance</th><td>{symbol}{b['credit_balance']:,.2f}</td></tr>
            <tr><th>Status</th><td class='{b['status_class']}'>{b['status']}</td></tr>
        </table>
    </div>
    """
    return render_page(content)

@app.route('/reports')
@login_required('admin')
def reports():
    symbol = currency_symbol()
    rows_data = db_execute("SELECT s.student_code, s.full_name, s.class_name, s.total_fee, COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid FROM students s LEFT JOIN payments p ON s.id = p.student_id GROUP BY s.id ORDER BY s.full_name ASC", fetchall=True) or []
    rows = ""
    for r in rows_data:
        b = balance_breakdown(r['total_fee'], r['amount_paid'])
        rows += f"<tr><td>{r['student_code'] or '-'}</td><td>{r['full_name']}</td><td>{r['class_name']}</td><td>{symbol}{r['total_fee']:,.2f}</td><td>{symbol}{r['amount_paid']:,.2f}</td><td>{symbol}{b['amount_due']:,.2f}</td><td>{symbol}{b['credit_balance']:,.2f}</td><td class='{b['status_class']}'>{b['status']}</td></tr>"
    export_btn = f"<div class='top-actions'><a href='{url_for('export_csv')}' class='btn btn-secondary'>Export CSV</a></div>"
    return render_page(export_btn + f"<table><tr><th>Student ID</th><th>Name</th><th>Class</th><th>Total Fee</th><th>Paid</th><th>Amount Due</th><th>Credit Balance</th><th>Status</th></tr>{rows}</table>")

@app.route('/export-csv')
@login_required('admin')
def export_csv():
    rows = db_execute("SELECT s.student_code, s.full_name, s.class_name, s.parent_phone, s.parent_email, s.total_fee, COALESCE(SUM(CASE WHEN p.status = 'Paid' THEN p.amount_paid ELSE 0 END), 0) AS amount_paid FROM students s LEFT JOIN payments p ON s.id = p.student_id GROUP BY s.id ORDER BY s.full_name ASC", fetchall=True) or []
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Student ID', 'Student Name', 'Class', 'Parent Phone', 'Parent Email', 'Total Fee', 'Amount Paid', 'Amount Due', 'Credit Balance', 'Status'])
    for row in rows:
        b = balance_breakdown(row['total_fee'], row['amount_paid'])
        writer.writerow([row['student_code'], row['full_name'], row['class_name'], row['parent_phone'], row['parent_email'], row['total_fee'], row['amount_paid'], b['amount_due'], b['credit_balance'], b['status']])
    mem = io.BytesIO()
    mem.write(output.getvalue().encode('utf-8'))
    mem.seek(0)
    output.close()
    return send_file(mem, mimetype='text/csv', as_attachment=True, download_name='school_fee_report.csv')

@app.route('/settings', methods=['GET', 'POST'])
@login_required('admin')
def settings_page():
    if request.method == 'POST':
        school_name = request.form['school_name'].strip() or DEFAULT_SCHOOL_NAME
        currency = request.form['currency'].strip() or DEFAULT_CURRENCY
        school_logo_text = request.form['school_logo'].strip()
        uploaded_logo = request.files.get('school_logo_file')
        remove_logo = request.form.get('remove_logo') == 'yes'
        current_logo = get_setting('school_logo', '')
        final_logo = '' if remove_logo else current_logo
        if uploaded_logo and uploaded_logo.filename:
            final_logo = file_to_data_url(uploaded_logo)
        elif school_logo_text:
            final_logo = school_logo_text
        set_setting('school_name', school_name)
        set_setting('school_logo', final_logo)
        set_setting('currency', currency)
        flash('School branding settings updated successfully.')
        return redirect(url_for('settings_page'))
    current_school_name = get_setting('school_name', DEFAULT_SCHOOL_NAME)
    current_school_logo = get_setting('school_logo', '')
    current_currency = get_setting('currency', DEFAULT_CURRENCY)
    logo_preview = f"<img src='{current_school_logo}' alt='School Logo' style='max-width:140px;max-height:140px;border:1px solid #ddd;border-radius:10px;padding:8px;background:#fff;'>" if current_school_logo else "<p class='muted'>No logo uploaded yet.</p>"
    return render_page(f"<form method='post' enctype='multipart/form-data'><div class='grid'><div><label>School Name</label><input name='school_name' value='{current_school_name}'></div><div><label>Currency</label><input name='currency' value='{current_currency}'></div></div><div class='card'>{logo_preview}</div><label>Upload School Logo</label><input type='file' name='school_logo_file'><label>Or Paste School Logo URL / Base64 Image</label><textarea name='school_logo'></textarea><label>Remove Existing Logo</label><select name='remove_logo'><option value='no'>No</option><option value='yes'>Yes</option></select><button type='submit'>Save Branding</button></form>")

@app.route('/search')
@login_required()
def global_search():
    query = request.args.get('q', '').strip()
    if not query:
        flash('Enter something to search.')
        return redirect(url_for('dashboard'))
    symbol = currency_symbol()
    students = db_execute("SELECT s.*, COALESCE(SUM(CASE WHEN py.status = 'Paid' THEN py.amount_paid ELSE 0 END), 0) AS amount_paid FROM students s LEFT JOIN payments py ON py.student_id = s.id WHERE s.full_name ILIKE %s OR s.student_code ILIKE %s OR s.class_name ILIKE %s GROUP BY s.id ORDER BY s.full_name ASC", (f'%{query}%', f'%{query}%', f'%{query}%'), fetchall=True) or []
    rows = ""
    for s in students:
        b = balance_breakdown(s['total_fee'], s['amount_paid'])
        rows += f"<tr><td>{s['student_code'] or '-'}</td><td>{s['full_name']}</td><td>{s['class_name']}</td><td>{symbol}{s['total_fee']:,.2f}</td><td>{symbol}{s['amount_paid']:,.2f}</td><td>{symbol}{b['amount_due']:,.2f}</td><td>{symbol}{b['credit_balance']:,.2f}</td><td class='{b['status_class']}'>{b['status']}</td></tr>"
    rows = rows or "<tr><td colspan='8'>No student record found.</td></tr>"
    return render_page(f"<table><tr><th>Student ID</th><th>Name</th><th>Class</th><th>Total Fee</th><th>Paid</th><th>Amount Due</th><th>Credit Balance</th><th>Status</th></tr>{rows}</table>")

@app.route('/health')
def health():
    school_name, _ = school_context()
    return jsonify({'ok': True, 'app': school_name, 'time': now_str()})

init_db()

if __name__ == '__main__':
    app.run(debug=(APP_ENV == 'development'))
