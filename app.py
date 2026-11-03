import os, uuid, logging, glob, zipfile, certifi, gc, shutil, time, subprocess, math, tempfile, hmac, hashlib, re
from datetime import datetime, timedelta
from flask import Flask, request, send_file, jsonify, session, redirect, url_for
from werkzeug.utils import secure_filename
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
from yt_dlp import YoutubeDL
import json
import requests
import stripe
import boto3
from botocore.exceptions import ClientError

from threading import Thread, BoundedSemaphore
from collections import deque

import resend
from openai import OpenAI

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from xhtml2pdf import pisa

os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

# Add ffmpeg_bin directory to PATH so pyacoustid and other subprocesses can find it
ffmpeg_bin_dir = os.path.join(os.getcwd(), 'ffmpeg_bin')
os.environ["PATH"] = ffmpeg_bin_dir + os.path.pathsep + os.environ.get("PATH", "")

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-prod')
app.config['SESSION_COOKIE_SAMESITE'] = 'None'
app.config['SESSION_COOKIE_SECURE'] = True
 
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY')

# Render Postgres Compatibility Fix
db_url = os.environ.get('DATABASE_URL', 'sqlite:///mp3audio.db')
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)

# Verify PostgreSQL connection works before using it
if db_url.startswith("postgresql://"):
    from sqlalchemy import create_engine
    try:
        # Check connection quickly with a 3-second timeout
        temp_engine = create_engine(db_url, connect_args={'connect_timeout': 3})
        with temp_engine.connect() as conn:
            pass
        temp_engine.dispose()
        logger.warning("Successfully connected to PostgreSQL database.")
    except Exception as db_err:
        logger.warning(f"Failed to connect to PostgreSQL ({db_err}). Falling back to local SQLite database.")
        db_url = 'sqlite:///mp3audio.db'

app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
if db_url.startswith("postgresql://"):
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_size': 10,
        'max_overflow': 20,
        'pool_timeout': 30,
        'pool_recycle': 1800,
        'connect_args': {'connect_timeout': 10}
    }

allowed_origins = [
    "https://mp3aud.io",
    "https://www.mp3aud.io",
    "https://mp3audio-staging.onrender.com",
    "https://mp3audio-staging-frontend.onrender.com",
    "http://localhost:3000",
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "http://localhost:5173",
    "http://127.0.0.1:5173"
]

CORS(app, supports_credentials=True, resources={
    r"/*": { "origins": allowed_origins, "methods": ["GET", "POST", "OPTIONS"], "allow_headers": ["Content-Type", "Authorization", "X-Admin-Secret"] }
})

db = SQLAlchemy(app)
serializer = URLSafeTimedSerializer(app.config['SECRET_KEY'])

@app.after_request
def ensure_cors_headers(response):
    origin = request.headers.get('Origin')
    if origin in allowed_origins:
        if 'Access-Control-Allow-Origin' not in response.headers:
            response.headers['Access-Control-Allow-Origin'] = origin
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Admin-Secret'
    return response

# Global handler for OPTIONS requests to ensure preflight checks never fail with 404/405
@app.route('/', defaults={'path': ''}, methods=['OPTIONS'])
@app.route('/<path:path>', methods=['OPTIONS'])
def handle_global_options(path):
    return jsonify({"status": "ok"}), 200

@app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Not Found', 'message': 'The requested URL was not found on the server.'}), 404

@app.errorhandler(Exception)
def handle_exception(e):
    import traceback
    return jsonify({
        'error': 'Internal Server Error',
        'message': str(e),
        'traceback': traceback.format_exc()
    }), 500

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    paid_track_credits = db.Column(db.Integer, default=0)
    stripe_customer_id = db.Column(db.String(120), nullable=True)
    subscription_active = db.Column(db.Boolean, default=False)
    free_video_used = db.Column(db.Boolean, default=False)

class ConversionJob(db.Model):
    id = db.Column(db.String(120), primary_key=True)
    user_id = db.Column(db.Integer)
    payment_method = db.Column(db.String(50))
    
    # Queue & Status
    status = db.Column(db.String(20), default='queued') # queued, processing, completed, error, cancelled
    priority = db.Column(db.Integer, default=0) # 1 for credits, 0 for free
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_update = db.Column(db.Float, default=time.time)
    
    # Processing State
    total = db.Column(db.Integer, default=0)
    completed = db.Column(db.Integer, default=0)
    skipped = db.Column(db.Integer, default=0)
    current_track = db.Column(db.Integer, default=0)
    sub_progress = db.Column(db.Integer, default=0)
    current_status = db.Column(db.String(255), default='')
    current_thumbnail = db.Column(db.String(500), default='')
    error = db.Column(db.Text, default='')
    email_summaries = db.Column(db.Text, default='')
    zip_ready = db.Column(db.Boolean, default=False)
    zip_path = db.Column(db.String(500), default='')
    
    # JSON Data (Task payload and results)
    entries = db.Column(db.JSON, default=list)
    completed_tracks = db.Column(db.JSON, default=list)
    failed_track_details = db.Column(db.JSON, default=list)
    
    # User Input Metadata
    url = db.Column(db.String(500))
    user_email = db.Column(db.String(120), nullable=True)
    start_time = db.Column(db.String(20), nullable=True)
    end_time = db.Column(db.String(20), nullable=True)
    transcribe_audio = db.Column(db.Boolean, default=False)
    increase_quality = db.Column(db.Boolean, default=False)
    organize_genre = db.Column(db.Boolean, default=False)
    auto_add_album_art = db.Column(db.Boolean, default=False)
    video_to_mp3 = db.Column(db.Boolean, default=False)
    is_ringtone = db.Column(db.Boolean, default=False)
    free_video_used = db.Column(db.Boolean, default=False)

class PopularURL(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String(500), unique=True, nullable=False)
    title = db.Column(db.String(200))
    artist = db.Column(db.String(200))
    conversion_count = db.Column(db.Integer, default=1)
    last_converted = db.Column(db.DateTime, default=datetime.utcnow)
    thumbnail_url = db.Column(db.String(500), nullable=True)

class SiteStats(db.Model):
    __tablename__ = 'site_stats'
    id = db.Column(db.Integer, primary_key=True)
    total_tracks = db.Column(db.Integer, default=0)

is_db_initialized = False

def initialize_database():
    """Runs database setup in the background to prevent boot stalling."""
    global is_db_initialized
    with app.app_context():
        try:
            db.create_all()

            # Initialize SiteStats if not present
            try:
                stats = SiteStats.query.first()
                if not stats:
                    try:
                        result = db.session.execute(text('SELECT SUM(completed) FROM conversion_job')).scalar()
                        initial_tracks = int(result) if result else 0
                    except Exception as bootstrap_err:
                        logger.warning(f"Could not bootstrap stats from conversion_job: {bootstrap_err}")
                        initial_tracks = 0
                    stats = SiteStats(total_tracks=initial_tracks)
                    db.session.add(stats)
                    db.session.commit()
            except Exception as stats_init_err:
                db.session.rollback()
                logger.warning(f"SiteStats initialization failed: {stats_init_err}")
            
            # --- AUTO MIGRATION FOR NEW COLUMNS ---
            # Adds missing columns to existing databases without requiring Alembic
            try:
                if 'postgresql' in app.config['SQLALCHEMY_DATABASE_URI']:
                    db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN IF NOT EXISTS increase_quality BOOLEAN DEFAULT FALSE'))
                    db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN IF NOT EXISTS organize_genre BOOLEAN DEFAULT FALSE'))
                    db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN IF NOT EXISTS auto_add_album_art BOOLEAN DEFAULT FALSE'))
                    db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN IF NOT EXISTS video_to_mp3 BOOLEAN DEFAULT FALSE'))
                    db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN IF NOT EXISTS is_ringtone BOOLEAN DEFAULT FALSE'))
                    db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN IF NOT EXISTS free_video_used BOOLEAN DEFAULT FALSE'))
                    db.session.execute(text('ALTER TABLE popular_url ADD COLUMN IF NOT EXISTS thumbnail_url VARCHAR(500)'))
                    db.session.commit()
                else: # Fallback for local SQLite testing
                    try: db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN increase_quality BOOLEAN DEFAULT FALSE'))
                    except: pass
                    try: db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN organize_genre BOOLEAN DEFAULT FALSE'))
                    except: pass
                    try: db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN auto_add_album_art BOOLEAN DEFAULT FALSE'))
                    except: pass
                    try: db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN video_to_mp3 BOOLEAN DEFAULT FALSE'))
                    except: pass
                    try: db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN is_ringtone BOOLEAN DEFAULT FALSE'))
                    except: pass
                    try: db.session.execute(text('ALTER TABLE conversion_job ADD COLUMN free_video_used BOOLEAN DEFAULT FALSE'))
                    except: pass
                    try: db.session.execute(text('ALTER TABLE popular_url ADD COLUMN thumbnail_url VARCHAR(500)'))
                    except: pass
            except Exception as e:
                db.session.rollback()
                logger.warning(f"Auto-migration skipped or failed: {e}")
                
            # Auto-migration for Stripe Subscription fields and free video tracking
            try:
                if 'postgresql' in app.config['SQLALCHEMY_DATABASE_URI']:
                    db.session.execute(text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS stripe_customer_id VARCHAR(120)'))
                    db.session.execute(text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS subscription_active BOOLEAN DEFAULT FALSE'))
                    db.session.execute(text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS free_video_used BOOLEAN DEFAULT FALSE'))
                    db.session.commit()
                else:
                    try: db.session.execute(text('ALTER TABLE user ADD COLUMN stripe_customer_id VARCHAR(120)'))
                    except: pass
                    try: db.session.execute(text('ALTER TABLE user ADD COLUMN subscription_active BOOLEAN DEFAULT FALSE'))
                    except: pass
                    try: db.session.execute(text('ALTER TABLE user ADD COLUMN free_video_used BOOLEAN DEFAULT FALSE'))
                    except: pass
            except Exception as e:
                db.session.rollback()
                logger.warning(f"Auto-migration skipped or failed: {e}")
                
            # Auto-migration to drop obsolete free_conversions_used column
            try:
                if 'postgresql' in app.config['SQLALCHEMY_DATABASE_URI']:
                    db.session.execute(text('ALTER TABLE "user" DROP COLUMN IF EXISTS free_conversions_used'))
                    db.session.commit()
                else:
                    try: db.session.execute(text('ALTER TABLE user DROP COLUMN free_conversions_used'))
                    except: pass
            except Exception as e:
                db.session.rollback()
                logger.warning(f"Drop column skipped or failed: {e}")

            # SYSTEM REBOOT RECOVERY: Refund credits for jobs interrupted by a sudden crash
            zombie_jobs = ConversionJob.query.filter(ConversionJob.status.in_(['queued', 'processing'])).all()
            if zombie_jobs:
                for z_job in zombie_jobs:
                    user = User.query.get(z_job.user_id) if z_job.user_id else None
                    if getattr(z_job, 'free_video_used', False) and z_job.completed == 0 and user:
                        user.free_video_used = False
                    unused_tracks = z_job.total - z_job.completed
                    if user and unused_tracks > 0:
                        total_paid = max(0, z_job.total - 5) * 1
                        if getattr(z_job, 'video_to_mp3', False): total_paid += z_job.total * 5
                        if z_job.auto_add_album_art: total_paid += max(0, z_job.total - 5) * 1
                        if z_job.increase_quality: total_paid += z_job.total * 1
                        if z_job.transcribe_audio: total_paid += z_job.total * 10
                        
                        used_spent = max(0, z_job.completed - 5) * 1
                        if getattr(z_job, 'video_to_mp3', False): used_spent += z_job.completed * 5
                        if z_job.auto_add_album_art: used_spent += max(0, z_job.completed - 5) * 1
                        if z_job.increase_quality: used_spent += z_job.completed * 1
                        if z_job.transcribe_audio: used_spent += z_job.completed * 10
                        
                        unused_credits = max(0, total_paid - used_spent)
                        if z_job.payment_method == 'credits':
                            user.paid_track_credits += unused_credits
                    z_job.status = 'error'
                    z_job.error = 'Job interrupted by server reboot.'
                db.session.commit()
                logger.warning(f"Recovered and refunded {len(zombie_jobs)} jobs interrupted by server reboot.")
            
            is_db_initialized = True
        except Exception as e:
            logger.error(f"Database initialization delayed or failed: {e}")


# Trigger DB setup in a background thread to prevent Gunicorn startup blocking
Thread(target=initialize_database, daemon=True).start()

DOWNLOAD_FOLDER = os.path.join(os.getcwd(), 'downloads')
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
try: client = OpenAI()
except: client = None

MAX_SONGS = 350
AVG_TIME_PER_TRACK = 45  
PUBLIC_URL = os.environ.get('PUBLIC_URL', 'https://mp3aud.io')
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'https://mp3aud.io').strip()
if not FRONTEND_URL.startswith('http'):
    FRONTEND_URL = 'https://' + FRONTEND_URL

# --- AWS S3 Configuration ---
S3_BUCKET = os.environ.get('S3_BUCKET_NAME')
s3_client = None
if S3_BUCKET:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_REGION'),
        endpoint_url=os.environ.get('AWS_ENDPOINT_URL')
    )

def setup_s3_lifecycle():
    """Configures Backblaze B2 / S3 to auto-delete files and old versions after 14 days."""
    if s3_client and S3_BUCKET:
        try:
            s3_client.put_bucket_lifecycle_configuration(
                Bucket=S3_BUCKET,
                LifecycleConfiguration={
                    'Rules': [
                        {
                            'ID': 'AutoDelete14Days',
                            'Filter': {'Prefix': 'downloads/'},
                            'Status': 'Enabled',
                            # Deletes active files after 14 days (fallback if your 1-hour cleanup fails)
                            'Expiration': {'Days': 14},
                            # CRITICAL: Deletes hidden/old versions that Backblaze keeps by default
                            'NoncurrentVersionExpiration': {'NoncurrentDays': 14},
                            # Cleans up failed/stuck multipart uploads to save space
                            'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7}
                        },
                        {
                            'ID': 'AutoDelete14DaysDeleteMarkers',
                            'Filter': {'Prefix': 'downloads/'},
                            'Status': 'Enabled',
                            # Required by Backblaze B2: an ExpiredObjectDeleteMarker rule is paired with the Expiration rule for the same prefix
                            'Expiration': {'ExpiredObjectDeleteMarker': True}
                        }
                    ]
                }
            )
        except Exception as e:
            logger.warning(f"Could not automatically apply S3 lifecycle rule: {e}")

# Run the S3 lifecycle setup in the background on startup
Thread(target=setup_s3_lifecycle, daemon=True).start()

def cleanup_memory(): gc.collect()

def cleanup_old_sessions():
    try:
        with app.app_context():
            current_time = time.time()
            threshold_time = current_time - 3600
            stuck_threshold = current_time - (3600 * 3) # 3 hours
            
            # 1. Clean up jobs that finished over 1 hour ago
            old_jobs = ConversionJob.query.filter(
                ConversionJob.status.notin_(['processing', 'queued']),
                ConversionJob.last_update < threshold_time
            ).all()
            
            for job in old_jobs:
                session_dir = os.path.join(DOWNLOAD_FOLDER, job.id)
                if os.path.exists(session_dir): shutil.rmtree(session_dir, ignore_errors=True)
                
                # Clean up S3 object to prevent paying for endless cloud storage
                if s3_client and S3_BUCKET:
                    try:
                        s3_client.delete_object(Bucket=S3_BUCKET, Key=f"downloads/{job.id}/playlist_backup.zip")
                    except Exception as e:
                        logger.error(f"S3 Delete failed: {e}")
                        
                db.session.delete(job)
                
            # 2. Catch ZOMBIE jobs stuck in 'processing' for over 3 hours
            stuck_jobs = ConversionJob.query.filter(
                ConversionJob.status == 'processing',
                ConversionJob.last_update < stuck_threshold
            ).all()
            
            for job in stuck_jobs:
                if getattr(job, 'free_video_used', False) and job.completed == 0:
                    user_to_restore = User.query.get(job.user_id) if job.user_id else None
                    if user_to_restore:
                        user_to_restore.free_video_used = False
                unused_tracks = job.total - job.completed
                if unused_tracks > 0:
                    total_paid = max(0, job.total - 5) * 1
                    if getattr(job, 'video_to_mp3', False): total_paid += job.total * 5
                    if job.auto_add_album_art: total_paid += max(0, job.total - 5) * 1
                    if job.increase_quality: total_paid += job.total * 1
                    if job.transcribe_audio: total_paid += job.total * 10
                    
                    used_spent = max(0, job.completed - 5) * 1
                    if getattr(job, 'video_to_mp3', False): used_spent += job.completed * 5
                    if job.auto_add_album_art: used_spent += max(0, job.completed - 5) * 1
                    if job.increase_quality: used_spent += job.completed * 1
                    if job.transcribe_audio: used_spent += job.completed * 10
                        
                    refund_credits = max(0, total_paid - used_spent)
                    refund_unused_credits(job.user_id, job.payment_method, refund_credits)
                job.status = 'error'
                job.error = 'Job timed out and was cancelled by the system.'
                job.last_update = time.time() # Reset timer so it gets deleted in the next hourly sweep
                
            db.session.commit()
    except Exception as e:
        logger.error(f"Automated cleanup error: {e}")

def automated_cleanup_loop():
    while True:
        time.sleep(900) # Run every 15 minutes
        cleanup_old_sessions()

# Start the automated cleanup timer in the background
Thread(target=automated_cleanup_loop, daemon=True).start()

def send_email_notification(recipient, subject, html_content):
    print(f"--- Attempting to send email to {recipient} ---", flush=True)
    resend_key = os.environ.get('RESEND_API_KEY')
    if not resend_key:
        err_msg = "RESEND_API_KEY is not set in environment variables."
        print(f"ERROR: {err_msg}", flush=True)
        return False, err_msg
        
    raw_from = os.environ.get('FROM_EMAIL', 'notifications@mail.mp3aud.io').strip()
    if '<' in raw_from and '>' in raw_from:
        from_email = raw_from
    else:
        from_email = f"MP3aud.io <{raw_from}>"

    payload = {
        "from": from_email,
        "to": [recipient],
        "subject": subject,
        "html": html_content
    }

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {resend_key.strip()}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=10
        )
        print(f"Resend HTTP Status: {resp.status_code}, Body: {resp.text}", flush=True)
        if resp.status_code in (200, 201):
            return True, None
        else:
            return False, f"Resend API Error ({resp.status_code}): {resp.text}"
    except Exception as e:
        err_msg = f"Failed to connect to Resend: {str(e)}"
        print(f"RESEND EXCEPTION: {err_msg}", flush=True)
        return False, err_msg

def get_or_create_user():
    # Check Authorization header first (for cookie-blocked environments like mobile Safari)
    auth_header = request.headers.get('Authorization')
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split(' ')[1]
        try:
            user_id = serializer.loads(token, salt='user-session')
            user = User.query.get(user_id)
            if user:
                session['user_id'] = user.id
                return user
        except Exception:
            pass

    if 'user_id' in session:
        user = User.query.get(session['user_id'])
        if user: return user
        
    fake_email = f"anon_{uuid.uuid4().hex[:12]}@guest.local"
    ghost_user = User(email=fake_email)
    db.session.add(ghost_user)
    db.session.commit()
    session['user_id'] = ghost_user.id
    return ghost_user

def refund_unused_credits(user_id, payment_method, unused_credits, session_id=None):
    try:
        with app.app_context():
            if unused_credits > 0 and user_id and payment_method:
                user = User.query.get(user_id)
                if user:
                    if payment_method == 'credits':
                        user.paid_track_credits += unused_credits
            db.session.commit()
    except Exception as e:
        logger.error(f"Failed to refund credits: {e}")

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route('/auth/login', methods=['POST'])
def send_magic_link():
    email = request.json.get('email', '').strip().lower()
    if not email: return jsonify({"error": "Email is required"}), 400
        
    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(email=email)
        db.session.add(user)
        db.session.commit()
        
    token = serializer.dumps(email, salt='magic-link')
    
    # Determine base frontend URL: prioritize Origin header if trusted, else FRONTEND_URL
    origin = request.headers.get('Origin', '').rstrip('/')
    base_url = FRONTEND_URL.rstrip('/')
    if origin and origin in allowed_origins:
        base_url = origin

    magic_url = f"{base_url}/?token={token}"
    
    email_subject = "Secure Login - MP3aud.io"
    html = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 30px; max-width: 600px; margin: 0 auto; border: 1px solid #e2e8f0; border-top: 4px solid #f59e0b; border-radius: 12px; background-color: #ffffff; box-shadow: 0 12px 30px rgba(0,0,0,0.05);">
        <h2 style="margin: 0 0 15px 0; color: #1e293b; font-size: 22px; font-weight: 800;">Log In & Manage Account</h2>
        <p style="color: #475569; font-size: 15px; margin-bottom: 24px; line-height: 1.5;">Click the button below to securely log in to your account. Once inside, you can access your credits or manage your subscription.</p>
        <a href="{magic_url}" style="background: linear-gradient(90deg, #f59e0b 0%, #4f46e5 100%); background-color: #4f46e5; color: white; padding: 12px 24px; text-decoration: none; border-radius: 8px; display: inline-block; font-weight: bold; font-size: 16px; box-shadow: 0 4px 10px rgba(139, 92, 246, 0.3);">Log In Securely</a>
        <p style="color: #94a3b8; font-size: 12px; margin-top: 25px; line-height: 1.4;">If you didn't request this link, you can safely ignore this email. The link will expire in 1 hour.</p>
    </div>"""
    
    success, err = send_email_notification(email, email_subject, html)
    if not success:
        logger.error(f"Magic link delivery failed: {err}")
        return jsonify({"error": f"Failed to send email. {err}"}), 500

    return jsonify({"success": True, "message": "Magic link sent to your email."})

@app.route('/auth/verify', methods=['POST'])
def verify_magic_link():
    token = request.json.get('token')
    if not token: return jsonify({"error": "No token provided"}), 400
    try: 
        email = serializer.loads(token, salt='magic-link', max_age=3600)
    except: 
        return jsonify({"error": "Invalid or expired link"}), 400
    user = User.query.filter_by(email=email).first()
    if user:
        session['user_id'] = user.id
        session_token = serializer.dumps(user.id, salt='user-session')
        return jsonify({
            "success": True,
            "token": session_token
        })
    return jsonify({"error": "User not found"}), 404

@app.route('/auth/me', methods=['GET'])
def get_current_user():
    user = get_or_create_user()
    is_guest = user.email.startswith('anon_')
    session_token = serializer.dumps(user.id, salt='user-session')
    
    has_used_free_video = getattr(user, 'free_video_used', False)
    if not has_used_free_video:
        prior_video_job = ConversionJob.query.filter_by(user_id=user.id, video_to_mp3=True).filter(ConversionJob.status.in_(['queued', 'processing', 'completed'])).first()
        if prior_video_job:
            has_used_free_video = True
            user.free_video_used = True
            db.session.commit()

    return jsonify({
        "authenticated": not is_guest,
        "email": None if is_guest else user.email,
        "paid_track_credits": user.paid_track_credits,
        "subscription_active": getattr(user, 'subscription_active', False),
        "free_video_used": has_used_free_video,
        "token": session_token
    })

@app.route('/auth/logout', methods=['POST'])
def logout():
    session.pop('user_id', None)
    return jsonify({"success": True})

@app.route('/create-checkout-session', methods=['POST'])
def create_checkout_session():
    user = get_or_create_user()
    if user.email.startswith('anon_'):
        return jsonify({"error": "Unauthorized. Please log in first."}), 401
        
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[{
                'price': os.environ.get('STRIPE_CREDITS_PRICE_ID'), 
                'quantity': 1,
            }],
            mode='payment',
            success_url=f"{FRONTEND_URL.rstrip('/')}/?success=true",
            cancel_url=f"{FRONTEND_URL.rstrip('/')}/?canceled=true",
            client_reference_id=str(user.id),
            customer_email=user.email
        )
        return jsonify({"url": session.url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/webhook/stripe', methods=['POST'])
def stripe_webhook():
    payload = request.data
    sig_header = request.headers.get('Stripe-Signature')
    endpoint_secret = os.environ.get('STRIPE_WEBHOOK_SECRET')

    if not endpoint_secret:
        return jsonify({'error': 'Webhook secret not configured'}), 400

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, endpoint_secret)
    except ValueError as e:
        return jsonify({'error': 'Invalid payload'}), 400
    except stripe.error.SignatureVerificationError as e:
        return jsonify({'error': 'Invalid signature'}), 400

    if event['type'] == 'checkout.session.completed':
        session_obj = event['data']['object']
        user_id = session_obj.get('client_reference_id')
        mode = session_obj.get('mode')
        
        if user_id:
            try:
                user = User.query.get(int(user_id))
                if user:
                    if mode == 'payment':
                        user.paid_track_credits += 350
                    elif mode == 'subscription':
                        user.subscription_active = True
                        user.stripe_customer_id = session_obj.get('customer')
                    db.session.commit()
            except Exception as e:
                logger.error(f"Error processing webhook user update: {e}")
                
    elif event['type'] == 'customer.subscription.deleted':
        subscription = event['data']['object']
        customer_id = subscription.get('customer')
        if customer_id:
            user = User.query.filter_by(stripe_customer_id=customer_id).first()
            if user:
                user.subscription_active = False
                db.session.commit()

    return jsonify({"status": "OK"}), 200

def notify_user_complete(session_id, user_email, track_count, html_summaries=""):
    if not user_email: return
    download_link = f"{PUBLIC_URL.rstrip('/')}/download/{session_id}/playlist_backup.zip"
    manuals_section = f"<div style='margin-top: 25px; padding: 20px; background-color: rgba(255,255,255,0.6); border-radius: 8px; border: 1px solid #fcd34d; color: #92400e;'>{html_summaries}</div>" if html_summaries else ""
    html = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 30px; max-width: 600px; margin: 0 auto; border: 2px solid #fde68a; border-radius: 12px; background-color: #fffbeb;">
        <h2 style="margin: 0 0 15px 0; color: #92400e; font-size: 22px; font-weight: 800;">Your Files Are Ready</h2>
        <p style="color: #92400e; font-size: 15px; margin-bottom: 24px; line-height: 1.5;">Your conversion of <strong>{track_count} media file(s)</strong> has finished processing.</p>
        <a href="{download_link}" style="background-color: #ea580c; color: white; padding: 12px 24px; text-decoration: none; border-radius: 8px; display: inline-block; font-weight: bold; font-size: 16px;">Download ZIP Archive</a>
        {manuals_section}
        <p style="color: #b45309; font-size: 12px; margin-top: 25px; line-height: 1.4;">This download link is secure and will automatically expire in 1 hour.</p>
    </div>
    """
    send_email_notification(user_email, "Your Conversion is Ready 📦", html)

def transcribe_audio_file(mp3_file_path, job=None):
    if not client: return None, None
    try:
        temp_dir = tempfile.mkdtemp()
        chunk_pattern = os.path.join(temp_dir, "chunk_%03d.mp3")
        ffmpeg_exe = 'ffmpeg_bin/ffmpeg' if os.path.exists('ffmpeg_bin/ffmpeg') else 'ffmpeg'
        if job: 
            job.current_status = 'Slicing audio for AI analysis...'
            db.session.commit()
        subprocess.run([ffmpeg_exe, '-y', '-i', mp3_file_path, '-f', 'segment', '-segment_time', '900', '-c', 'copy', chunk_pattern], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        
        chunks = sorted(glob.glob(os.path.join(temp_dir, "chunk_*.mp3")))
        total_chunks = len(chunks)
        full_transcript = ""
        
        for i, chunk_path in enumerate(chunks):
            if job:
                job.current_status = f'Transcribing audio (Part {i+1} of {total_chunks})...'
                job.sub_progress = int((i / total_chunks) * 100)
                db.session.commit()
            try:
                with open(chunk_path, "rb") as audio_file:
                    transcript = client.audio.transcriptions.create(model="whisper-1", file=audio_file)
                full_transcript += transcript.text + " "
            except Exception as e:
                logger.error(f"Failed to transcribe chunk {i}: {e}")
                full_transcript += f"\n[Warning: AI transcription failed for this segment.]\n"
        
        if job: 
            job.sub_progress = 100
            db.session.commit()
        shutil.rmtree(temp_dir, ignore_errors=True)
                
        text_file_path = mp3_file_path.replace('.mp3', '.txt')
        with open(text_file_path, "w", encoding="utf-8") as f: f.write(full_transcript.strip()) 
            
        pdf_file_path = mp3_file_path.replace('.mp3', '.pdf')
        try:
            doc = SimpleDocTemplate(pdf_file_path, pagesize=letter)
            story = [Paragraph(full_transcript.strip().replace('\n', '<br/>'), getSampleStyleSheet()["Normal"])]
            doc.build(story)
        except Exception as e:
            logger.error(f"PDF creation failed: {e}")
            pdf_file_path = None
        return text_file_path, pdf_file_path
    except Exception as e:
        logger.error(f"Transcription process failed: {e}")
        return None, None

def generate_diy_manual(transcript_text_path, job=None):
    if not client: return None, None, None
    try:
        if job: 
            job.current_status = 'Formatting AI summary...'
            job.sub_progress = 0
            db.session.commit()
        with open(transcript_text_path, "r", encoding="utf-8") as file: transcript = file.read()[:100000] 
        system_prompt = "You are an expert technical writer. Format the provided text into a highly detailed, comprehensive document in HTML format."
        response = client.chat.completions.create(
            model="gpt-4o", 
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": f"Here is the raw transcript:\n\n{transcript}"}],
            temperature=0.3 
        )
        if job: 
            job.sub_progress = 100
            db.session.commit()
        manual_html = response.choices[0].message.content
        manual_path = transcript_text_path.replace('.txt', '_summary.html')
        pdf_path = transcript_text_path.replace('.txt', '_summary.pdf')
        with open(manual_path, "w", encoding="utf-8") as f: f.write(manual_html)
        try:
            with open(pdf_path, "w+b") as result_file: pisa.CreatePDF(manual_html, dest=result_file)
        except Exception as e:
            logger.error(f"PDF creation failed: {e}")
            pdf_path = None
        return manual_path, pdf_path, manual_html
    except Exception as e:
        logger.error(f"Manual generation failed: {e}")
        return None, None, None

def resolve_track_metadata(file_path, original_title, original_artist):
    """Resolves track title and artist using existing tags, filename parsing, and audio fingerprinting."""
    title = original_title
    artist = original_artist
    album = "Unknown Album"
    year = None
    
    # Step 1: Read existing metadata tags using ffprobe (non-intrusive metadata check)
    try:
        ffprobe_exe = 'ffmpeg_bin/ffprobe' if os.path.exists('ffmpeg_bin/ffprobe') else 'ffprobe'
        probe_cmd = [ffprobe_exe, '-v', 'quiet', '-probesize', '50M', '-analyzeduration', '100M', '-print_format', 'json', '-show_format', file_path]
        probe_out = subprocess.check_output(probe_cmd)
        probe_data = json.loads(probe_out)
        tags = probe_data.get('format', {}).get('tags', {})
        tags_lower = {k.lower(): v for k, v in tags.items()}
        
        if tags_lower.get('title'):
            title = tags_lower.get('title')
        if tags_lower.get('artist'):
            artist = tags_lower.get('artist')
        if tags_lower.get('album'):
            album = tags_lower.get('album')
        for date_key in ['date', 'year', 'tyer', 'tdrc', 'tdat', 'recording_time']:
            if tags_lower.get(date_key):
                ym = re.search(r'\b(19\d\d|20\d\d)\b', str(tags_lower[date_key]))
                if ym:
                    year = ym.group(1)
                    break
    except Exception:
        pass
        
    # Step 2: Fall back to filename parsing if title/artist is generic or missing
    is_generic = not title or title.lower() in ["unknown track", "audio", "track", "sound", "download", "unnamed"]
    if is_generic or not artist or artist.lower() == "unknown artist":
        filename = os.path.splitext(os.path.basename(file_path))[0]
        # Skip generic filename words
        if filename.lower() not in ["audio", "track", "sound", "download", "unnamed"]:
            if " - " in filename:
                parts = filename.split(" - ", 1)
                artist = parts[0].replace('_', ' ').strip()
                title = parts[1].replace('_', ' ').strip()
            else:
                title = filename.replace('_', ' ').strip()

    # Step 3: Fall back to AcoustID/Chromaprint audio fingerprinting if it's still generic
    is_still_generic = not title or title.lower() in ["unknown track", "audio", "track", "sound", "download", "unnamed"]
    acoustid_api_key = os.environ.get("ACOUSTID_API_KEY")
    if is_still_generic and acoustid_api_key:
        try:
            import acoustid
            results = acoustid.match(acoustid_api_key, file_path)
            for score, recording_id, r_title, r_artist in results:
                if score > 0.6:  # 60% confidence threshold
                    title = r_title
                    artist = r_artist
                    break
        except Exception as e:
            logger.warning(f"AcoustID audio fingerprinting lookup failed: {e}")
            
    return title, artist, album, year

def fetch_album_art_from_itunes(track_title, artist_name=None):
    """Queries iTunes Search API for album artwork URL, returning updated metadata and high-res cover URL."""
    if not track_title or not isinstance(track_title, str):
        return None

    def clean_query(text):
        if not text:
            return ""
        # Remove file extensions
        text = re.sub(r'\.(mp3|wav|flac|m4a|aac|ogg|wma)$', '', text, flags=re.IGNORECASE)
        # Remove common video/audio tag noise
        text = re.sub(r'[\(\[\{].*?(official|video|lyric|lyrics|audio|remaster|hd|4k|version|edit|visualizer|clip|full).*?[\)\]\}]', '', text, flags=re.IGNORECASE)
        text = re.sub(r'\b(official video|official audio|lyrics|lyric video|hd|4k|full song)\b', '', text, flags=re.IGNORECASE)
        text = text.replace('_', ' ')
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    clean_title = clean_query(track_title)
    clean_artist = clean_query(artist_name) if artist_name and artist_name != "Unknown Artist" else ""

    queries = []
    if clean_artist and clean_title:
        queries.append(f"{clean_artist} {clean_title}")
    if clean_title:
        queries.append(clean_title)
    if track_title and track_title not in queries:
        queries.append(track_title)

    for q in queries:
        try:
            url = "https://itunes.apple.com/search"
            params = {
                "term": q,
                "media": "music",
                "entity": "song",
                "limit": 1
            }
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if results:
                    result = results[0]
                    artwork_url = result.get("artworkUrl100", "")
                    if artwork_url:
                        # Upgrade resolution from 100x100 to 1000x1000 for maximum quality album art
                        high_res_url = artwork_url.replace("100x100bb.jpg", "1000x1000bb.jpg")
                        release_date = result.get("releaseDate")
                        track_year = None
                        if release_date:
                            ym = re.search(r'\b(19\d\d|20\d\d)\b', str(release_date))
                            if ym:
                                track_year = ym.group(1)
                        return {
                            "artwork_url": high_res_url,
                            "track_name": result.get("trackName", track_title),
                            "artist_name": result.get("artistName", artist_name or "Unknown Artist"),
                            "album_name": result.get("collectionName", "Single / Unknown Album"),
                            "year": track_year
                        }
        except Exception as e:
            logger.warning(f"iTunes Search API lookup failed for term '{q}': {e}")
            
    return None

def download_image(url, temp_dir):
    """Downloads an image file from a URL to a temporary local file."""
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            ext = 'jpg'
            content_type = response.headers.get('Content-Type', '')
            if 'png' in content_type:
                ext = 'png'
            temp_img_path = os.path.join(temp_dir, f"cover_{uuid.uuid4().hex[:8]}.{ext}")
            with open(temp_img_path, 'wb') as f:
                f.write(response.content)
            return temp_img_path
    except Exception as e:
        logger.warning(f"Failed to download artwork from {url}: {e}")
    return None

def process_track(url, session_dir, track_index, ffmpeg_exe, session_id, zip_path, track_name, artist_name, thumbnail, start_time, end_time, transcribe_audio, increase_quality=False, organize_genre=False, auto_add_album_art=False, video_to_mp3=False, is_ringtone=False):
    job = ConversionJob.query.get(session_id)
    if not job or job.status == 'cancelled': return False

    temp_filename_base = f"track_{track_index}"
    last_commit_time = [time.time()]
    
    def progress_hook(d):
        if job.status == 'cancelled': 
            raise Exception("CancelledByUser")
            
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate')
            if total and d.get('downloaded_bytes'):
                job.sub_progress = int((d['downloaded_bytes'] / total) * 100)
            job.current_status = 'Downloading audio...'
        elif d['status'] == 'finished':
            job.sub_progress = 100
            job.current_status = 'Extracting audio...'
            
        if time.time() - last_commit_time[0] > 1.0 or d['status'] == 'finished':
            job.last_update = time.time()
            db.session.commit()
            last_commit_time[0] = time.time()

    ydl_opts = {
        'format': 'http_mp3_128/bestaudio[ext=mp3]/bestaudio/best',
        'outtmpl': os.path.join(session_dir, f"{temp_filename_base}.%(ext)s"),
        'ffmpeg_location': ffmpeg_exe,
        'quiet': True, 'no_warnings': True, 'nocheckcertificate': True,
        'socket_timeout': 30, 'retries': 5,
        'hls_prefer_native': True, 
        'writethumbnail': True,
        'progress_hooks': [progress_hook], 'cookiefile': None,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        },
        'postprocessors': [
            {'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '320' if increase_quality else '192'},
            {'key': 'EmbedThumbnail'},
        ],
        'postprocessor_args': {
            'ffmpeg': [
                '-map_metadata', '-1', 
                '-threads', '1',
                '-err_detect', 'ignore_err'
            ]
        },
    }

    if start_time or end_time:
        ydl_opts['external_downloader'] = ffmpeg_exe
        ffmpeg_args = ['-y']
        if start_time:
            ffmpeg_args.extend(['-ss', str(start_time)])
        if end_time:
            ffmpeg_args.extend(['-to', str(end_time)])
        ydl_opts['external_downloader_args'] = {'ffmpeg_i': ffmpeg_args}

    is_local_file = url.startswith('local:')
    local_path = url[6:] if is_local_file else None
    original_ext = local_path.split('.')[-1].lower() if is_local_file and '.' in local_path else 'mp3'
    
    album_name = "Unknown Album"
    track_year = None

    # Extract metadata immediately for local files before doing anything else
    if is_local_file:
        try:
            ffprobe_exe = 'ffmpeg_bin/ffprobe' if os.path.exists('ffmpeg_bin/ffprobe') else 'ffprobe'
            probe_cmd = [ffprobe_exe, '-v', 'quiet', '-probesize', '50M', '-analyzeduration', '100M', '-print_format', 'json', '-show_format', local_path]
            probe_out = subprocess.check_output(probe_cmd)
            probe_data = json.loads(probe_out)
            tags = probe_data.get('format', {}).get('tags', {})
            tags_lower = {k.lower(): v for k, v in tags.items()}
            
            if tags_lower.get('artist'): artist_name = tags_lower.get('artist')
            if tags_lower.get('album'): album_name = tags_lower.get('album')
            if tags_lower.get('title'): track_name = tags_lower.get('title')
            for date_key in ['date', 'year', 'tyer', 'tdrc', 'tdat', 'recording_time']:
                if tags_lower.get(date_key):
                    ym = re.search(r'\b(19\d\d|20\d\d)\b', str(tags_lower[date_key]))
                    if ym:
                        track_year = ym.group(1)
                        break
        except Exception as e:
            pass

    try:
        job.current_track = track_index
        job.last_update = time.time()
        job.current_status = f'Initializing track {track_index}...'
        job.sub_progress = 0
        job.current_thumbnail = thumbnail 
        db.session.commit()
        
        if job.status == 'cancelled': return False

        file_to_zip = None
        
        if not is_local_file:
            try:
                with YoutubeDL({'quiet':True, 'no_warnings':True, 'socket_timeout':10}) as ydl:
                    info = ydl.extract_info(url, download=False)
                    if info and 'entries' in info and info['entries']: info = info['entries'][0]
                    if info.get('title') and (not track_name or track_name == "Unknown Track"): track_name = info['title']
                    if info.get('uploader') and (not artist_name or artist_name in ("Unknown Artist", "Artist")): artist_name = info['uploader']
                    if info.get('album'): album_name = info['album']
                    if info.get('thumbnail'): job.current_thumbnail = info['thumbnail']
                    if info.get('release_year'):
                        track_year = str(info['release_year'])
                    elif info.get('release_date'):
                        ym = re.search(r'\b(19\d\d|20\d\d)\b', str(info['release_date']))
                        if ym: track_year = ym.group(1)
                    elif info.get('upload_date'):
                        ym = re.search(r'\b(19\d\d|20\d\d)\b', str(info['upload_date']))
                        if ym: track_year = ym.group(1)
                    db.session.commit()
            except: pass
            
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            
            mp3_files = glob.glob(os.path.join(session_dir, f"{temp_filename_base}*.mp3"))
            if mp3_files:
                file_to_zip = mp3_files[0]
                original_ext = 'mp3'
        else:
            video_extensions = {'mp4', 'mov', 'm4v', 'webm', 'mkv', 'avi', 'flv', 'wmv', '3gp', 'ts'}
            is_video = (original_ext in video_extensions) or video_to_mp3

            if increase_quality or is_video or is_ringtone:
                # Standardize to MP3. If increase_quality is True, use 320k; otherwise standard 192k.
                original_ext = 'mp3'
                file_to_zip = os.path.join(session_dir, f"{temp_filename_base}.mp3")
                bitrate = '320k' if increase_quality else '192k'
                cmd = [ffmpeg_exe, '-y', '-probesize', '50M', '-analyzeduration', '100M']
                if start_time:
                    cmd.extend(['-ss', str(start_time)])
                if end_time:
                    cmd.extend(['-to', str(end_time)])
                cmd.extend(['-i', local_path, '-vn', '-c:a', 'libmp3lame', '-b:a', bitrate])
            else:
                # NEVER convert format unless requested: just strip video/art and copy raw audio
                file_to_zip = os.path.join(session_dir, f"{temp_filename_base}.{original_ext}")
                cmd = [ffmpeg_exe, '-y', '-probesize', '50M', '-analyzeduration', '100M']
                if start_time:
                    cmd.extend(['-ss', str(start_time)])
                if end_time:
                    cmd.extend(['-to', str(end_time)])
                cmd.extend(['-i', local_path, '-vn', '-c:a', 'copy'])
                
            cmd.append(file_to_zip)
            
            try:
                subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"FFmpeg local processing failed for {local_path}: {e.stderr}")
                err_lines = [line for line in (e.stderr or '').strip().split('\n') if line.strip()]
                last_err = err_lines[-1] if err_lines else 'Unknown format or corrupted file'
                raise Exception(f"FFmpeg processing failed: {last_err}")
                
            if not os.path.exists(file_to_zip):
                file_to_zip = None

        if file_to_zip and os.path.exists(file_to_zip):
            
            # Resolve track metadata if auto_add_album_art is requested, or if metadata is unknown
            cover_path = None
            if auto_add_album_art:
                resolved_title, resolved_artist, resolved_album, resolved_year = resolve_track_metadata(file_to_zip, track_name, artist_name)
                if resolved_year and not track_year:
                    track_year = resolved_year
                
                # Fetch artwork and full track info from iTunes Search API
                itunes_info = fetch_album_art_from_itunes(resolved_title, resolved_artist)
                if not itunes_info and (track_name != resolved_title or artist_name != resolved_artist):
                    itunes_info = fetch_album_art_from_itunes(track_name, artist_name)

                if itunes_info:
                    track_name = itunes_info["track_name"]
                    artist_name = itunes_info["artist_name"]
                    album_name = itunes_info["album_name"]
                    if itunes_info.get("year"):
                        track_year = itunes_info["year"]
                    
                    # Download cover artwork from iTunes to embed it
                    cover_path = download_image(itunes_info["artwork_url"], session_dir)
                    if cover_path:
                        # Update current thumbnail to show the fetched album art in the progress bar
                        thumbnail = itunes_info["artwork_url"]
                        if job:
                            job.current_thumbnail = thumbnail
                            db.session.commit()
                elif thumbnail and thumbnail.startswith('http'):
                    # Fall back to downloading provided thumbnail if iTunes returns no match
                    cover_path = download_image(thumbnail, session_dir)
                    track_name = resolved_title
                    artist_name = resolved_artist
                    album_name = resolved_album
                    if resolved_year:
                        track_year = resolved_year
                else:
                    # Fall back to using cleaner resolved title/artist
                    track_name = resolved_title
                    artist_name = resolved_artist
                    album_name = resolved_album
                    if resolved_year:
                        track_year = resolved_year

            # 1. TRANSCRIBE (if requested) so we have lyrics to physically embed
            lyrics_text = ""
            raw_pdf_to_zip = None
            if transcribe_audio:
                raw_txt_path, raw_pdf_path = transcribe_audio_file(file_to_zip, job)
                if raw_txt_path and os.path.exists(raw_txt_path):
                    with open(raw_txt_path, 'r', encoding='utf-8') as f:
                        lyrics_text = f.read()
                raw_pdf_to_zip = raw_pdf_path
                
            # Get original extension to avoid format detection failure with .tmp extension
            original_ext = file_to_zip.split('.')[-1].lower()
            temp_output = file_to_zip + '.tmp.' + original_ext

            # 2. METADATA PASS (Title, Artist, and Lyrics) & Cover Art Embedding
            try:
                if cover_path and os.path.exists(cover_path):
                    # Embed cover art and copy audio stream
                    cmd = [ffmpeg_exe, '-y', '-i', file_to_zip, '-i', cover_path]
                    # Map the audio stream from input 0 and the image stream from input 1
                    cmd.extend(['-map', '0:a', '-map', '1:0', '-c', 'copy', '-disposition:v:0', 'attached_pic'])
                    if original_ext == 'mp3':
                        cmd.extend(['-id3v2_version', '3', '-metadata:s:v', 'title=Album cover', '-metadata:s:v', 'comment=Cover (front)'])
                else:
                    cmd = [ffmpeg_exe, '-y', '-i', file_to_zip]
                    cmd.extend(['-map', '0', '-c', 'copy'])
                    
                if track_name and track_name != "Unknown Track":
                    cmd.extend(['-metadata', f'title={track_name}'])
                if artist_name and artist_name != "Unknown Artist":
                    cmd.extend(['-metadata', f'artist={artist_name}'])
                if album_name and album_name != "Unknown Album":
                    cmd.extend(['-metadata', f'album={album_name}'])
                if track_year:
                    cmd.extend(['-metadata', f'date={track_year}'])
                if lyrics_text:
                    cmd.extend(['-metadata', f'lyrics={lyrics_text}'])
                    
                cmd.append(temp_output)
                
                # Capture stderr for better error reporting on exception
                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
                if os.path.exists(temp_output): 
                    os.replace(temp_output, file_to_zip)
            except Exception as e:
                err_msg = getattr(e, 'stderr', str(e))
                if isinstance(err_msg, bytes):
                    err_msg = err_msg.decode('utf-8', errors='ignore')
                logger.warning(f"Embedding failed or map failed, falling back to standard copy. Error: {err_msg}")
                try:
                    # Fallback copy command without video mapping if it failed (e.g. for unsupported formats)
                    fallback_cmd = [ffmpeg_exe, '-y', '-i', file_to_zip, '-map', '0', '-c', 'copy']
                    if track_name and track_name != "Unknown Track":
                        fallback_cmd.extend(['-metadata', f'title={track_name}'])
                    if artist_name and artist_name != "Unknown Artist":
                        fallback_cmd.extend(['-metadata', f'artist={artist_name}'])
                    if album_name and album_name != "Unknown Album":
                        fallback_cmd.extend(['-metadata', f'album={album_name}'])
                    if track_year:
                        fallback_cmd.extend(['-metadata', f'date={track_year}'])
                    if lyrics_text:
                        fallback_cmd.extend(['-metadata', f'lyrics={lyrics_text}'])
                    fallback_cmd.append(temp_output)
                    subprocess.run(fallback_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
                    if os.path.exists(temp_output):
                        os.replace(temp_output, file_to_zip)
                except Exception:
                    pass
            finally:
                if cover_path and os.path.exists(cover_path):
                    try:
                        os.remove(cover_path)
                    except:
                        pass

            folder_path = ""
            if organize_genre:
                clean_artist = "".join([c for c in artist_name if c.isalnum() or c in (' ', '-')]).strip() if artist_name and artist_name != "Unknown Artist" else "Unknown Artist"
                clean_album = "".join([c for c in album_name if c.isalnum() or c in (' ', '-')]).strip() if album_name and album_name != "Unknown Album" else "Unknown Album"
                folder_path = f"{clean_artist}/{clean_album}/"

            if folder_path:
                clean_name = "".join([c for c in track_name[:100] if c.isalnum() or c in (' ', '-', '_')]).strip() or f"Track_{track_index}"
            else:
                clean_name = "".join([c for c in f"{artist_name} - {track_name}"[:100] if c.isalnum() or c in (' ', '-', '_')]).strip() or f"Track_{track_index}"
            
            m4r_file = None
            if (is_ringtone or video_to_mp3) and file_to_zip and os.path.exists(file_to_zip):
                try:
                    m4r_file = os.path.join(session_dir, f"{temp_filename_base}_iphone.m4r")
                    cmd_m4r = [ffmpeg_exe, '-y', '-i', file_to_zip, '-vn', '-c:a', 'aac', '-b:a', '256k', m4r_file]
                    subprocess.run(cmd_m4r, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
                except Exception as e_m4r:
                    logger.warning(f"M4R generation failed: {e_m4r}")
                    m4r_file = None

            with zipfile.ZipFile(zip_path, 'a', zipfile.ZIP_STORED) as z:
                z.write(file_to_zip, f"{folder_path}{clean_name}.{original_ext}")
                if m4r_file and os.path.exists(m4r_file):
                    z.write(m4r_file, f"{folder_path}{clean_name}.m4r")
            
                if transcribe_audio:
                    if not is_local_file:
                        # Keep DIY Meeting Notes strictly for URL Downloads
                        html_path, summary_pdf_path, manual_html = generate_diy_manual(raw_txt_path, job)
                        if raw_pdf_to_zip and os.path.exists(raw_pdf_to_zip): z.write(raw_pdf_to_zip, f"{folder_path}{clean_name}_transcript.pdf")
                        if summary_pdf_path and os.path.exists(summary_pdf_path): z.write(summary_pdf_path, f"{folder_path}{clean_name}_summary.pdf")
                        if manual_html: job.email_summaries = (job.email_summaries or "") + f"<hr><h2>{clean_name}</h2>" + manual_html
                    else:
                        # For file uploads, simply attach the raw lyrics PDF 
                        if raw_pdf_to_zip and os.path.exists(raw_pdf_to_zip): z.write(raw_pdf_to_zip, f"{folder_path}{clean_name}_lyrics.pdf")

            job.completed += 1
            job.sub_progress = 100
            
            completed_list = list(job.completed_tracks)
            completed_list.append(clean_name)
            job.completed_tracks = completed_list
            
            # Increment global site stats persistently
            try:
                db.session.execute(text('UPDATE site_stats SET total_tracks = total_tracks + 1'))
            except Exception as stats_err:
                logger.warning(f"Failed to increment global stats: {stats_err}")
                
            db.session.commit()
            
            if is_local_file:
                try:
                    os.remove(local_path)
                except: pass
            
            return True
        else:
            if job.status != 'cancelled':
                job.skipped += 1
                
                failed_list = list(job.failed_track_details)
                failed_list.append({
                    "track": track_name or f"Track {track_index}",
                    "reason": "Corrupted stream or missing audio track."
                })
                job.failed_track_details = failed_list
                db.session.commit()
            return False

    except Exception as e:
        logger.error(f"Track processing error: {e}")
        if job.status != 'cancelled': 
            job.skipped += 1
            error_string = str(e)
            
            if "404" in error_string:
                friendly_reason = "Private, deleted, or invalid track link."
            elif "403" in error_string:
                friendly_reason = "Geo-blocked or access denied by platform."
            elif "ffmpeg processing failed" in error_string.lower():
                err_detail = error_string.split("FFmpeg processing failed:")[-1].strip()
                friendly_reason = f"Unsupported or corrupted file. ({err_detail})"
            elif "ffmpeg" in error_string.lower():
                friendly_reason = "Server audio processor (FFmpeg) missing."
            else:
                friendly_reason = "Unsupported format or protected track."

            failed_list = list(job.failed_track_details)
            failed_list.append({
                "track": track_name or f"Track {track_index}",
                "reason": friendly_reason
            })
            job.failed_track_details = failed_list
            db.session.commit()
        return False
        
    finally:
        try:
            for f in glob.glob(os.path.join(session_dir, f"{temp_filename_base}*")):
                try: os.remove(f)
                except: pass
        except: pass
        cleanup_memory()

def run_conversion_task(session_id):
    with app.app_context():
        job = ConversionJob.query.get(session_id)
        if not job: return
        
        session_dir = os.path.join(DOWNLOAD_FOLDER, session_id)
        os.makedirs(session_dir, exist_ok=True)
        zip_path = os.path.join(session_dir, "playlist_backup.zip")
        ffmpeg_exe = 'ffmpeg_bin/ffmpeg' if os.path.exists('ffmpeg_bin/ffmpeg') else 'ffmpeg'

        try:
            for idx, t_url, t_title, t_artist, t_thumb in job.entries:
                # Refresh job status from DB before starting the next track
                job = ConversionJob.query.get(session_id)
                if job.status == 'cancelled': break
                
                process_track(t_url, session_dir, idx, ffmpeg_exe, session_id, zip_path, t_title, t_artist, t_thumb, job.start_time, job.end_time, job.transcribe_audio, job.increase_quality, job.organize_genre, job.auto_add_album_art, getattr(job, 'video_to_mp3', False), getattr(job, 'is_ringtone', False))

            job = ConversionJob.query.get(session_id)
            if job.status != 'cancelled':
                job.status = 'completed'
                
                if job.completed > 0:
                    job.zip_ready = True
                    job.zip_path = f"/download/{session_id}/playlist_backup.zip"
                    
                    # Upload to S3 and immediately delete the local disk footprint
                    if s3_client and S3_BUCKET:
                        try:
                            s3_client.upload_file(zip_path, S3_BUCKET, f"downloads/{session_id}/playlist_backup.zip")
                            shutil.rmtree(session_dir, ignore_errors=True)
                        except Exception as e:
                            logger.error(f"S3 Upload failed: {e}")
                    
                    if job.user_email: notify_user_complete(session_id, job.user_email, job.completed, job.email_summaries)
            db.session.commit()
            
        except Exception as e:
            job = ConversionJob.query.get(session_id)
            if job:
                job.status = 'error'
                job.error = str(e)
                if getattr(job, 'free_video_used', False) and job.completed == 0:
                    user_to_restore = User.query.get(job.user_id) if job.user_id else None
                    if user_to_restore:
                        user_to_restore.free_video_used = False
                db.session.commit()
        finally:
            job = ConversionJob.query.get(session_id)
            if job and job.payment_method == 'credits':
                total_paid = max(0, job.total - 5) * 1
                if getattr(job, 'video_to_mp3', False): total_paid += job.total * 5
                if job.auto_add_album_art: total_paid += max(0, job.total - 5) * 1
                if job.increase_quality: total_paid += job.total * 1
                if job.transcribe_audio: total_paid += job.total * 10
                
                used_spent = max(0, job.completed - 5) * 1
                if getattr(job, 'video_to_mp3', False): used_spent += job.completed * 5
                if job.auto_add_album_art: used_spent += max(0, job.completed - 5) * 1
                if job.increase_quality: used_spent += job.completed * 1
                if job.transcribe_audio: used_spent += job.completed * 10
                
                refund_credits = max(0, total_paid - used_spent)
                refund_unused_credits(job.user_id, job.payment_method, refund_credits, session_id)
            
            cleanup_memory()

def worker_loop():
    while True:
        try:
            session_id = None
            with app.app_context():
                # Find the next job (Priority 1 first, then oldest)
                query = ConversionJob.query.filter_by(status='queued').order_by(
                    ConversionJob.priority.desc(), ConversionJob.created_at.asc()
                )
                
                # Use skip_locked to avoid race conditions with multiple workers
                if 'postgresql' in app.config['SQLALCHEMY_DATABASE_URI']:
                    query = query.with_for_update(skip_locked=True)
                    
                job = query.first()
                
                if job:
                    job.status = 'processing'
                    job.last_update = time.time()
                    db.session.commit()
                    
                    session_id = job.id
                    
            if session_id:
                run_conversion_task(session_id)
            else:
                time.sleep(1)
        except Exception as e: 
            logger.error(f"Worker queue error: {e}")
            time.sleep(1)

queue_worker = Thread(target=worker_loop, daemon=True)
queue_worker.start()

# Renamed to avoid adblockers that strictly block requests containing the word "upload"
@app.route('/process_local_files', methods=['POST'])
def process_local_files():
    user = get_or_create_user()
    session_id = request.form.get('session_id', str(uuid.uuid4()))
    
    if 'files' not in request.files:
        return jsonify({"error": "No files provided"}), 400
        
    uploaded_files = request.files.getlist('files')
    if not uploaded_files or uploaded_files[0].filename == '':
        return jsonify({"error": "No files selected"}), 400

    total_tracks = len(uploaded_files)
    
    increase_quality = request.form.get('increase_quality') == 'true'
    attach_lyrics = request.form.get('attach_lyrics') == 'true'
    organize_genre = request.form.get('organize_genre') == 'true'
    auto_add_album_art = request.form.get('auto_add_album_art') == 'true'
    video_to_mp3 = request.form.get('video_to_mp3') == 'true'
    is_ringtone = request.form.get('is_ringtone') == 'true'
    start_time = request.form.get('start_time')
    end_time = request.form.get('end_time')

    video_extensions = {'mp4', 'mov', 'm4v', 'webm', 'mkv', 'avi', 'flv', 'wmv', '3gp', 'ts'}
    video_count = 0
    for file in uploaded_files:
        ext = file.filename.split('.')[-1].lower() if '.' in file.filename else ''
        if ext in video_extensions:
            video_count += 1

    if video_count > 0:
        video_to_mp3 = True
    if video_to_mp3:
        is_ringtone = True

    # Check if user has already used their first free video to mp3 conversion
    has_used_free_video = getattr(user, 'free_video_used', False)
    if not has_used_free_video:
        prior_video_job = ConversionJob.query.filter_by(user_id=user.id, video_to_mp3=True).filter(ConversionJob.status.in_(['queued', 'processing', 'completed'])).first()
        if prior_video_job:
            has_used_free_video = True
            user.free_video_used = True
            db.session.commit()

    free_video_applied = False
    payable_video_count = 0

    if video_to_mp3 and video_count > 0:
        if not has_used_free_video:
            free_video_applied = True
            payable_video_count = max(0, video_count - 1)
        else:
            payable_video_count = video_count
    elif video_to_mp3 and video_count == 0:
        if not has_used_free_video and total_tracks > 0:
            free_video_applied = True
            payable_video_count = max(0, total_tracks - 1)
        else:
            payable_video_count = total_tracks

    total_credits_needed = max(0, total_tracks - 5) * 1
    total_credits_needed += payable_video_count * 5

    if auto_add_album_art: total_credits_needed += max(0, total_tracks - 5) * 1
    if increase_quality: total_credits_needed += total_tracks * 1
    if attach_lyrics: total_credits_needed += total_tracks * 10
    
    is_premium_job = attach_lyrics or increase_quality or (payable_video_count > 0)
    
    payment_method = None
    if not is_premium_job and total_tracks <= 5:
        payment_method = 'always_free'
        if free_video_applied:
            user.free_video_used = True
            db.session.commit()
    elif user.paid_track_credits >= total_credits_needed:
        user.paid_track_credits -= total_credits_needed
        payment_method = 'credits'
        if free_video_applied:
            user.free_video_used = True
            db.session.commit()
    else:
        if free_video_applied:
            error_detail = f"Your first video is free! Converting {video_count} video file(s) requires {total_credits_needed} credits for the {payable_video_count} additional video(s) (5 credits each)."
        elif video_to_mp3 and video_count > 0:
            error_detail = f"Video to MP3 conversion requires {total_credits_needed} credits ({video_count} video file(s) at 5 credits each)."
        else:
            error_detail = f"This action requires {total_credits_needed} credits."

        return jsonify({
            "error": f"{error_detail} Please log in and purchase credits.", 
            "requires_payment": True
        }), 403

    session_dir = os.path.join(DOWNLOAD_FOLDER, session_id, 'uploads')
    os.makedirs(session_dir, exist_ok=True)
    
    valid_entries = []
    for i, file in enumerate(uploaded_files):
        original_name = file.filename
        filename = secure_filename(original_name)
        
        # Ensure we always keep a proper extension and valid name
        ext = original_name.split('.')[-1].lower() if '.' in original_name else 'mp3'
        if not filename or filename == ext or filename.startswith('.'):
            filename = f"track_{i+1}_{int(time.time())}.{ext}"
            
        file_path = os.path.join(session_dir, filename)
        file.save(file_path)
        valid_entries.append((i+1, f"local:{file_path}", filename, "Unknown Artist", ""))

    queue_position = ConversionJob.query.filter_by(status='queued').count() + 1
    job_priority = 1 if payment_method == 'credits' else 0

    new_job = ConversionJob(id=session_id, user_id=user.id, payment_method=payment_method, status='queued', priority=job_priority, total=total_tracks, entries=valid_entries, url="File Upload", user_email=user.email if not user.email.startswith('anon_') else None, start_time=start_time, end_time=end_time, transcribe_audio=attach_lyrics, increase_quality=increase_quality, organize_genre=organize_genre, auto_add_album_art=auto_add_album_art, video_to_mp3=video_to_mp3, is_ringtone=is_ringtone, free_video_used=free_video_applied)
    db.session.add(new_job)
    db.session.commit()
    return jsonify({"session_id": session_id, "total_tracks": total_tracks, "status": "queued", "queue_position": queue_position}), 200

@app.route('/start_conversion', methods=['POST'])
def start_conversion():
    user = get_or_create_user()
    data = request.json
    raw_url = data.get('url', '').strip()
    url = raw_url.split('?')[0] if raw_url else ''
    session_id = data.get('session_id', str(uuid.uuid4()))
    if not url: return jsonify({"error": "No URL provided"}), 400
    
    playlist_title = 'Audio URL'
    playlist_artist = ''
    playlist_thumbnail = ''
    
    try:
        with YoutubeDL({'extract_flat': True, 'quiet': True, 'playlistend': MAX_SONGS, 'nocheckcertificate': True}) as ydl:
            info = ydl.extract_info(url, download=False)
            entries = info.get('entries', [info]) if info else []
            valid_entries = []
            for i, e in enumerate(entries[:MAX_SONGS]):
                if e:
                    track_url = e.get('url') or e.get('webpage_url') or e.get('id', '')
                    if not track_url.startswith('http') and 'soundcloud' in url: 
                        track_url = f"https://api.soundcloud.com/tracks/{e.get('id', i)}"
                    elif not track_url.startswith('http'): continue 
                    valid_entries.append((i+1, track_url, e.get('title', f"Track {i+1}"), e.get('uploader', 'Artist'), e.get('thumbnail', '')))
            total_tracks = len(valid_entries)

        if total_tracks == 0: return jsonify({"error": "No tracks found or supported."}), 400
        
        playlist_title = info.get('title')
        if not playlist_title and entries:
            playlist_title = entries[0].get('title', 'Unknown Title')
            
        playlist_artist = info.get('uploader')
        if not playlist_artist and entries:
            playlist_artist = entries[0].get('uploader', 'Unknown Artist')
            
        playlist_thumbnail = info.get('thumbnail')
        if not playlist_thumbnail and info.get('thumbnails'):
            playlist_thumbnail = info['thumbnails'][-1].get('url')
        if not playlist_thumbnail and entries:
            playlist_thumbnail = entries[0].get('thumbnail')
            
    except Exception as e:
        valid_entries = [(1, url, "Unknown Track", "Unknown Artist", "")]
        total_tracks = 1
        
    transcribe_audio = data.get('transcribe_audio', False)
    increase_quality = data.get('increase_quality', False)
    organize_genre = data.get('organize_genre', False)
    auto_add_album_art = data.get('auto_add_album_art', False)

    total_credits_needed = max(0, total_tracks - 5) * 1
    if auto_add_album_art: total_credits_needed += max(0, total_tracks - 5) * 1
    if increase_quality: total_credits_needed += total_tracks * 1
    if transcribe_audio: total_credits_needed += total_tracks * 10
    
    is_premium_job = transcribe_audio or increase_quality
    
    payment_method = None
    if not is_premium_job and total_tracks <= 5:
        payment_method = 'always_free'
    elif user.paid_track_credits >= total_credits_needed:
        user.paid_track_credits -= total_credits_needed
        payment_method = 'credits'
    else:
        return jsonify({
            "error": f"This action requires {total_credits_needed} credits. Please log in and purchase credits.", 
            "requires_payment": True
        }), 403

    
    # --- TRACK POPULAR URL ---

    if auto_add_album_art:
        popular_url = PopularURL.query.filter_by(url=url).first()
        if popular_url:
            popular_url.conversion_count += 1
            popular_url.last_converted = datetime.utcnow()
            if not popular_url.thumbnail_url and playlist_thumbnail:
                popular_url.thumbnail_url = playlist_thumbnail[:500]
        else:
            popular_url = PopularURL(
                url=url, 
                title=playlist_title[:200] if playlist_title else 'Audio URL', 
                artist=playlist_artist[:200] if playlist_artist else '',
                thumbnail_url=playlist_thumbnail[:500] if playlist_thumbnail else ''
            )
            db.session.add(popular_url)
    # -------------------------
    
    queue_position = ConversionJob.query.filter_by(status='queued').count() + 1
    job_priority = 1 if payment_method == 'credits' else 0

    new_job = ConversionJob(
        id=session_id,
        user_id=user.id,
        payment_method=payment_method,
        status='queued',
        priority=job_priority,
        total=total_tracks,
        entries=valid_entries,
        url=url,
        user_email=user.email if not user.email.startswith('anon_') else None,
        start_time=data.get('start_time'),
        end_time=data.get('end_time'),
        transcribe_audio=transcribe_audio,
        increase_quality=increase_quality,
        organize_genre=organize_genre,
        auto_add_album_art=auto_add_album_art,
        video_to_mp3=data.get('video_to_mp3', False),
        is_ringtone=data.get('is_ringtone', False) or data.get('video_to_mp3', False)
    )
    db.session.add(new_job)
    db.session.commit()

    return jsonify({
        "session_id": session_id, 
        "total_tracks": total_tracks, 
        "status": "queued", 
        "queue_position": queue_position
    }), 200

@app.route('/status/<session_id>', methods=['GET'])
def get_status(session_id):
    job = ConversionJob.query.get(session_id)
    if not job: return jsonify({"error": "Session not found"}), 404
    queue_pos, wait_seconds = 0, 0
    
    if job.status == 'queued':
        # Count jobs ahead in the database queue
        ahead_count = ConversionJob.query.filter(
            ConversionJob.status == 'queued',
            db.or_(
                ConversionJob.priority > job.priority,
                db.and_(ConversionJob.priority == job.priority, ConversionJob.created_at < job.created_at)
            )
        ).count()
        queue_pos = ahead_count + 1
        wait_seconds = ahead_count * AVG_TIME_PER_TRACK * 10 # Approx 10 tracks avg

    return jsonify({
        "status": job.status, 
        "total": job.total, 
        "completed": job.completed, 
        "skipped": job.skipped, 
        "failed_details": job.failed_track_details,
        "current_track": job.current_track, 
        "current_status": job.current_status, 
        "current_thumbnail": job.current_thumbnail, 
        "zip_ready": job.zip_ready,
        "zip_path": job.zip_path, 
        "sub_progress": job.sub_progress,
        "error": job.error, 
        "queue_position": queue_pos, 
        "estimated_wait": math.ceil(wait_seconds / 60)
    }), 200

@app.route('/cancel', methods=['POST'])
def cancel_conversion():
    session_id = request.json.get('session_id')
    job = ConversionJob.query.get(session_id)
    if job:
        job.status = 'cancelled'
        if getattr(job, 'free_video_used', False) and job.completed == 0:
            user_to_restore = User.query.get(job.user_id) if job.user_id else None
            if user_to_restore:
                user_to_restore.free_video_used = False
        
        unused_tracks = job.total - job.completed
        if unused_tracks > 0:
            total_paid = max(0, job.total - 5) * 1
            if getattr(job, 'video_to_mp3', False): total_paid += job.total * 5
            if job.auto_add_album_art: total_paid += max(0, job.total - 5) * 1
            if job.increase_quality: total_paid += job.total * 1
            if job.transcribe_audio: total_paid += job.total * 10
            
            used_spent = max(0, job.completed - 5) * 1
            if getattr(job, 'video_to_mp3', False): used_spent += job.completed * 5
            if job.auto_add_album_art: used_spent += max(0, job.completed - 5) * 1
            if job.increase_quality: used_spent += job.completed * 1
            if job.transcribe_audio: used_spent += job.completed * 10
                
            refund_credits = max(0, total_paid - used_spent)
            refund_unused_credits(job.user_id, job.payment_method, refund_credits, session_id=None)
            
        db.session.commit()
        return jsonify({"status": "cancelling"}), 200
    return jsonify({"status": "not_found"}), 404

@app.route('/download/<session_id>/<filename>')
def download_file(session_id, filename):
    session_id = secure_filename(session_id)
    filename = secure_filename(filename)
    
    # Intercept the request and redirect to a secure AWS S3 URL if configured
    if s3_client and S3_BUCKET:
        try:
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': S3_BUCKET, 'Key': f"downloads/{session_id}/{filename}"},
                ExpiresIn=3600 # Link expires in 1 hour
            )
            return redirect(presigned_url)
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {e}")
            return "File not found", 404
            
    file_path = os.path.join(DOWNLOAD_FOLDER, session_id, filename)
    if os.path.exists(file_path): return send_file(file_path, as_attachment=True)
    return "File not found", 404

@app.route('/api/top-urls', methods=['GET', 'OPTIONS'])
def get_top_urls():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        limit = int(request.args.get('limit', 20))
        offset = int(request.args.get('offset', 0))
        # Get the most recently converted URLs
        top_urls = PopularURL.query.order_by(PopularURL.last_converted.desc()).limit(limit).offset(offset).all()
        result = []
        for item in top_urls:
            result.append({
                "title": item.title or "Unknown Title",
                "desc": item.artist or "Various Artists",
                "link": item.url,
                "date": item.last_converted.strftime("%b %d, %Y"),
                "count": item.conversion_count,
                "thumbnail_url": item.thumbnail_url or ""
            })
        return jsonify({"success": True, "data": result}), 200
    except Exception as e:
        logger.error(f"Error fetching top urls: {e}")
        # Fallback gracefully to an empty list so the frontend doesn't throw a red error
        # This handles cases where the DB table is still initializing
        return jsonify({"success": True, "data": [], "error": str(e)}), 200

@app.route('/api/stats', methods=['GET', 'OPTIONS'])
def get_stats():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        job_tracks = 0
        try:
            res_job = db.session.execute(text('SELECT SUM(completed) FROM conversion_job')).scalar()
            if res_job:
                job_tracks = int(res_job)
        except Exception as e1:
            logger.warning(f"Error querying conversion_job stats: {e1}")

        pop_tracks = 0
        try:
            res_pop = db.session.execute(text('SELECT SUM(conversion_count) FROM popular_url')).scalar()
            if res_pop:
                pop_tracks = int(res_pop)
        except Exception as e2:
            logger.warning(f"Error querying popular_url stats: {e2}")

        # Add baseline of 18450 plus completed conversion jobs and album art conversions
        total_tracks = 18450 + job_tracks + pop_tracks
        return jsonify({"success": True, "total_tracks": total_tracks}), 200
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        return jsonify({"success": True, "total_tracks": 18450}), 200

@app.route('/admin/jobs', methods=['GET'])
def admin_jobs():
    # Secure the route with a secret key
    admin_secret = os.environ.get('ADMIN_SECRET')
    provided_secret = request.headers.get('X-Admin-Secret') or request.args.get('secret')
    
    if admin_secret and provided_secret != admin_secret:
        return jsonify({"error": "Unauthorized. Invalid or missing admin secret."}), 401
        
    try:
        processing_jobs = ConversionJob.query.filter_by(status='processing').all()
        queued_jobs = ConversionJob.query.filter_by(status='queued').order_by(
            ConversionJob.priority.desc(), ConversionJob.created_at.asc()
        ).all()
        
        def format_job(job):
            return {
                "id": job.id,
                "user_email": job.user_email or "Guest",
                "payment_method": job.payment_method,
                "priority": job.priority,
                "progress": f"{job.completed} / {job.total}",
                "sub_progress_percent": job.sub_progress,
                "current_status": job.current_status,
                "created_at": job.created_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "last_update": datetime.fromtimestamp(job.last_update).strftime("%Y-%m-%d %H:%M:%S UTC")
            }
            
        return jsonify({
            "total_processing": len(processing_jobs),
            "total_queued": len(queued_jobs),
            "processing": [format_job(j) for j in processing_jobs],
            "queued": [format_job(j) for j in queued_jobs]
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health')
def health():
    check_db = request.args.get('check_db', '').lower() == 'true'
    if check_db:
        if not is_db_initialized:
            return jsonify({"status": "initializing", "database": "migrating"}), 503
        try:
            # Check database connection works
            db.session.execute(text('SELECT 1'))
            db.session.commit()
            return jsonify({"status": "ok", "database": "connected"}), 200
        except Exception as e:
            db.session.rollback()
            logger.error(f"Health check failed database ping: {e}")
            return jsonify({"status": "error", "database": "disconnected", "error": str(e)}), 500
    else:
        return jsonify({"status": "ok"}), 200

@app.route('/')
def index(): return jsonify({"message": "Audio Processor API", "status": "active"}), 200

@app.route('/api-version')
def api_version():
    return jsonify({"version": "v7_updated", "has_upload_route": True}), 200

from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import io

# ==============================================================================
# MP3AUD.IO PERSONAL MUSIC ASSISTANT & WHATSAPP / TELEGRAM MESSAGING ENGINE
# ==============================================================================

VIBE_PRESET_QUERIES = {
    "synthwave": ("Midnight Neon: Chill Synthwave Mix", "Warm analog synths, retro arpeggios & late-night drive energy", [
        ("Midnight City", "M83"), ("Nightcall", "Kavinsky"), ("Resonance", "Home"),
        ("After Dark", "Mr.Kitty"), ("Sunset", "The Midnight"), ("Tech Noir", "GUNSHIP")
    ]),
    "workout": ("High-Octane Gym & Workout Anthems", "Heavy bass, relentless tempo & peak adrenaline", [
        ("Stronger", "Kanye West"), ("Till I Collapse", "Eminem"), ("HUMBLE.", "Kendrick Lamar"),
        ("POWER", "Kanye West"), ("Eye of the Tiger", "Survivor"), ("Titanium", "David Guetta")
    ]),
    "hip hop": ("Golden Era & Modern Hip-Hop Essentials", "Boom-bap classics, sharp lyricism & iconic 808s", [
        ("N.Y. State of Mind", "Nas"), ("Shook Ones, Pt. II", "Mobb Deep"), ("C.R.E.A.M.", "Wu-Tang Clan"),
        ("Juicy", "The Notorious B.I.G."), (" Still D.R.E.", "Dr. Dre"), ("Money Trees", "Kendrick Lamar")
    ]),
    "chill": ("Late-Night Velvet & Chill Vibes", "Smooth grooves, dreamy textures & laid-back atmosphere", [
        ("Redbone", "Childish Gambino"), (" Pink + White", "Frank Ocean"), ("Electric Feel", "MGMT"),
        ("Chamber Of Reflection", "Mac DeMarco"), (" Get Lucky", "Daft Punk"), ("Best Part", "Daniel Caesar")
    ]),
    "lofi": ("Lo-Fi Study & Rainy Window Beats", "Dusty vinyl crackle, warm Rhodes chords & mellow boom-bap", [
        ("Snowman", "WYS"), ("Coffee", "beabadoobee"), ("5:32pm", "The Deli"),
        ("Affection", "Jinsang"), ("Feather", "Nujabes"), ("Luv(sic) Part 3", "Nujabes")
    ]),
    "acoustic": ("Warm Coffeehouse Acoustic Sessions", "Intimate fingerpicking, rich vocals & stripped-back storytelling", [
        ("Fast Car", "Tracy Chapman"), ("Banana Pancakes", "Jack Johnson"), ("Skinny Love", "Bon Iver"),
        ("Blackbird", "The Beatles"), ("Ho Hey", "The Lumineers"), ("Photograph", "Ed Sheeran")
    ]),
    "rock": ("Stadium & Classic Rock Legends", "Timeless guitar riffs, soaring vocals & driving drums", [
        ("Bohemian Rhapsody", "Queen"), ("Dreams", "Fleetwood Mac"), ("Hotel California", "Eagles"),
        ("Smells Like Teen Spirit", "Nirvana"), ("Mr. Brightside", "The Killers"), ("Everlong", "Foo Fighters")
    ]),
    "pop": ("Pop Supernova: Chart-Topping Hits", "Infectious hooks, glossy production & feel-good energy", [
        ("Blinding Lights", "The Weeknd"), ("Levitating", "Dua Lipa"), ("As It Was", "Harry Styles"),
        ("Cruel Summer", "Taylor Swift"), ("Espresso", "Sabrina Carpenter"), ("Starboy", "The Weeknd")
    ]),
    "jazz": ("Midnight Blue Note Jazz Lounge", "Smoky saxophone, walking upright bass & velvet trumpet", [
        ("Take Five", "The Dave Brubeck Quartet"), ("So What", "Miles Davis"), ("My Favorite Things", "John Coltrane"),
        ("Fly Me to the Moon", "Frank Sinatra"), ("Autumn Leaves", "Cannonball Adderley"), ("Blue in Green", "Miles Davis")
    ]),
    "edm": ("Festival Mainstage & Electronic Euphoria", "Massive builds, euphoric synths & club-ready drops", [
        ("Levels", "Avicii"), ("Strobe", "deadmau5"), ("One More Time", "Daft Punk"),
        ("Summer", "Calvin Harris"), ("Clarity", "Zedd"), ("Scary Monsters and Nice Sprites", "Skrillex")
    ]),
    "r&b": ("Silk & Soul: R&B Slow Jams", "Lush harmonies, late-night basslines & timeless soul", [
        ("Snooze", "SZA"), ("No Scrubs", "TLC"), ("Adorn", "Miguel"),
        ("Earned It", "The Weeknd"), ("Boo'd Up", "Ella Mai"), (" Kiss It Better", "Rihanna")
    ]),
}

def format_duration_ms(ms):
    if not ms or not isinstance(ms, (int, float)):
        return "3:30", 210
    sec = max(1, int(ms // 1000))
    return f"{sec // 60}:{sec % 60:02d}", sec

def format_itunes_item(item, index=1):
    title = item.get("trackName") or "Unknown Track"
    artist = item.get("artistName") or "Unknown Artist"
    album = item.get("collectionName") or "Single"
    artwork = (item.get("artworkUrl100") or "").replace("100x100bb.jpg", "600x600bb.jpg")
    preview_url = item.get("previewUrl") or ""
    release_date = item.get("releaseDate") or ""
    year = ""
    if release_date:
        ym = re.search(r'\b(19\d\d|20\d\d)\b', str(release_date))
        if ym:
            year = ym.group(1)
    dur_str, dur_sec = format_duration_ms(item.get("trackTimeMillis"))
    return {
        "index": index,
        "title": title,
        "artist": artist,
        "album": album,
        "year": year,
        "genre": item.get("primaryGenreName") or "Music",
        "duration": dur_str,
        "duration_sec": dur_sec,
        "artwork_url": artwork,
        "preview_url": preview_url,
        "search_query": f"ytsearch1:{artist} - {title} official audio"
    }

def enrich_single_track(title, artist="", index=1):
    query = f"{artist} {title}".strip() if artist else title.strip()
    try:
        resp = requests.get(
            "https://itunes.apple.com/search",
            params={"term": query, "media": "music", "entity": "song", "limit": 1},
            timeout=5
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return format_itunes_item(results[0], index=index)
    except Exception as e:
        logger.warning(f"iTunes enrich failed for '{query}': {e}")

    return {
        "index": index,
        "title": title or f"Track {index}",
        "artist": artist or "Featured Artist",
        "album": "Single",
        "year": "2025",
        "genre": "Music",
        "duration": "3:24",
        "duration_sec": 204,
        "artwork_url": "https://images.unsplash.com/photo-1511671782779-c97d3d27a1d4?auto=format&fit=crop&w=600&q=80",
        "preview_url": "",
        "search_query": f"ytsearch1:{artist} - {title} official audio".strip()
    }

def search_itunes_multiple(term, limit=6):
    try:
        resp = requests.get(
            "https://itunes.apple.com/search",
            params={"term": term, "media": "music", "entity": "song", "limit": limit},
            timeout=6
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            seen = set()
            tracks = []
            for item in results:
                key = (
                    (item.get("trackName") or "").lower().strip(),
                    (item.get("artistName") or "").lower().strip()
                )
                if key not in seen and item.get("trackName"):
                    seen.add(key)
                    tracks.append(format_itunes_item(item, index=len(tracks) + 1))
            return tracks
    except Exception as e:
        logger.warning(f"iTunes multi-search failed for '{term}': {e}")
    return []

def curate_music_response(user_message, history=None, options=None):
    """
    Core AI Music Assistant brain.
    Takes any text message (song name, artist, mood/vibe playlist request, ringtone request, or URL)
    and returns a structured conversational response with playable tracks and download metadata.
    """
    msg = (user_message or "").strip()
    if not msg:
        return {
            "success": False,
            "error": "Please enter a message, song, artist, or playlist vibe."
        }

    msg_lower = msg.lower()
    is_ringtone_req = any(w in msg_lower for w in ["ringtone", "m4r", "alarm tone", "text tone"])

    # 1. Check if the user pasted a direct URL (YouTube, SoundCloud, TikTok, etc.)
    url_match = re.search(r'(https?://[^\s]+)', msg)
    if url_match:
        raw_url = url_match.group(1).strip()
        clean_url = raw_url.split('?')[0] if 'soundcloud.com' in raw_url else raw_url
        tracks = []
        playlist_title = "Extracted Media Link"
        try:
            with YoutubeDL({'extract_flat': True, 'quiet': True, 'playlistend': 15, 'nocheckcertificate': True, 'socket_timeout': 10}) as ydl:
                info = ydl.extract_info(clean_url, download=False)
                if info:
                    playlist_title = info.get('title') or playlist_title
                    entries = info.get('entries', [info])
                    raw_items = [e for e in entries if e][:12]
                    for idx, e in enumerate(raw_items):
                        t_title = e.get('title') or f"Track {idx+1}"
                        t_artist = e.get('uploader') or info.get('uploader') or "Unknown Artist"
                        t_url = e.get('url') or e.get('webpage_url') or clean_url
                        if not str(t_url).startswith('http'):
                            t_url = clean_url
                        enriched = enrich_single_track(t_title, t_artist, index=idx+1)
                        enriched['search_query'] = t_url
                        if e.get('thumbnail') and not enriched.get('artwork_url'):
                            enriched['artwork_url'] = e['thumbnail']
                        tracks.append(enriched)
        except Exception as e:
            logger.warning(f"URL extraction in chat fallback: {e}")

        if not tracks:
            tracks = [{
                "index": 1,
                "title": "Media Stream Track",
                "artist": "Direct Link",
                "album": "Web Stream",
                "year": "2026",
                "genre": "Audio",
                "duration": "3:30",
                "duration_sec": 210,
                "artwork_url": "https://images.unsplash.com/photo-1511671782779-c97d3d27a1d4?auto=format&fit=crop&w=600&q=80",
                "preview_url": "",
                "search_query": clean_url
            }]

        mode = "ringtone" if is_ringtone_req else ("playlist" if len(tracks) > 1 else "single")
        reply = (
            f"I loaded **{playlist_title}** ({len(tracks)} {'tracks' if len(tracks) > 1 else 'track'}) from your link! "
            f"Tap play below to preview, or hit **Download MP3** {'/ **Download Playlist (.ZIP)** ' if len(tracks) > 1 else ''}"
            f"for full 320kbps audio with HD cover art."
        )
        return {
            "success": True,
            "reply": reply,
            "mode": mode,
            "playlist_title": playlist_title,
            "playlist_vibe": "Direct Link Extraction • High-Resolution ID3 Tagged",
            "tracks": tracks
        }

    # 2. Try OpenAI GPT-4o-mini / GPT-4o curation when configured
    wants_playlist = any(w in msg_lower for w in [
        "playlist", "mix", "songs", "tracks", "vibe", "vibes", "mood", "music for",
        "workout", "chill", "study", "party", "late night", "throwback", "top ", "best ",
        "similar to", "like ", "station", "radio", "list"
    ])

    if client:
        try:
            sys_prompt = (
                "You are MP3aud.io, an elite text-messaging Personal Music Assistant & AI DJ. "
                "When the user texts you for a song, artist, ringtone, or mood/vibe playlist, respond strictly in valid JSON with:\n"
                "{\n"
                '  "reply": "Warm, concise 1-2 sentence text message response introducing the track(s)",\n'
                '  "playlist_title": "Catchy title for the track or playlist",\n'
                '  "playlist_vibe": "Short 4-8 word vibe/genre subtitle",\n'
                '  "mode": "single" | "playlist" | "ringtone",\n'
                '  "tracks": [{"title": "Exact Song Title", "artist": "Exact Artist Name"}]\n'
                "}\n"
                "Rules:\n"
                "- If the user asks for a single specific song or ringtone, return 1 track (or up to 3 if they ask for options).\n"
                "- If the user asks for a playlist, vibe, mood, genre, or artist mix, return 6 curated real songs.\n"
                "- Output ONLY raw JSON."
            )
            messages_payload = [{"role": "system", "content": sys_prompt}]
            if history and isinstance(history, list):
                for h in history[-4:]:
                    if isinstance(h, dict) and h.get("role") in ("user", "assistant") and h.get("content"):
                        messages_payload.append({"role": h["role"], "content": str(h["content"])[:500]})
            messages_payload.append({"role": "user", "content": msg})

            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages_payload,
                temperature=0.6,
                response_format={"type": "json_object"}
            )
            parsed = json.loads(completion.choices[0].message.content)
            raw_tracks = parsed.get("tracks") or []
            if raw_tracks:
                with ThreadPoolExecutor(max_workers=6) as pool:
                    futures = [
                        pool.submit(enrich_single_track, t.get("title", ""), t.get("artist", ""), idx + 1)
                        for idx, t in enumerate(raw_tracks[:10])
                    ]
                    enriched_tracks = [f.result() for f in futures]
                return {
                    "success": True,
                    "reply": parsed.get("reply") or "Here are the tracks I curated for you! Tap play to listen in chat or download the MP3s below.",
                    "mode": "ringtone" if is_ringtone_req else (parsed.get("mode") or ("playlist" if len(enriched_tracks) > 1 else "single")),
                    "playlist_title": parsed.get("playlist_title") or "Curated Mix",
                    "playlist_vibe": parsed.get("playlist_vibe") or "AI Curated • 320kbps Ready",
                    "tracks": enriched_tracks
                }
        except Exception as ai_err:
            logger.warning(f"OpenAI music curation fell back to smart search: {ai_err}")

    # 3. Smart Natural Language Fallback (Works 100% without OpenAI API key!)
    # Check if message matches one of our rich vibe presets
    for vibe_key, (p_title, p_vibe, seed_list) in VIBE_PRESET_QUERIES.items():
        if vibe_key in msg_lower or (vibe_key == "lofi" and "lo-fi" in msg_lower):
            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = [
                    pool.submit(enrich_single_track, t_title, t_artist, idx + 1)
                    for idx, (t_title, t_artist) in enumerate(seed_list)
                ]
                enriched_tracks = [f.result() for f in futures]
            return {
                "success": True,
                "reply": f"Here is your **{p_title}**! Tap play on any audio bubble to listen right in the chat, download individual 320kbps MP3s, or grab the full playlist ZIP.",
                "mode": "playlist",
                "playlist_title": p_title,
                "playlist_vibe": p_vibe,
                "tracks": enriched_tracks
            }

    # Clean conversational filler words to extract the core artist / song / genre query
    cleaned_query = re.sub(
        r'^(hey|hi|hello|yo|please|can you|could you|send me|play|find|get me|make me|create|give me|i want|i need|text me)\s+',
        '',
        msg,
        flags=re.IGNORECASE
    )
    cleaned_query = re.sub(
        r'\b(a playlist of|a playlist for|playlist|songs by|tracks by|music by|ringtone of|ringtone for|ringtone|mp3|audio|song|track)\b',
        ' ',
        cleaned_query,
        flags=re.IGNORECASE
    )
    cleaned_query = re.sub(r'\s+', ' ', cleaned_query).strip() or msg

    search_limit = 6 if wants_playlist else (1 if is_ringtone_req else 4)
    tracks = search_itunes_multiple(cleaned_query, limit=search_limit)

    if not tracks:
        tracks = [enrich_single_track(cleaned_query, "", index=1)]

    if len(tracks) == 1 or is_ringtone_req:
        t = tracks[0]
        mode = "ringtone" if is_ringtone_req else "single"
        reply = (
            f"Here is **{t['title']}** by **{t['artist']}**! "
            f"{'Tap **Download Ringtone** to get your custom iPhone (.M4R) & Android (.MP3) ringtone, or hit play to preview it first.' if is_ringtone_req else 'Tap play in the bubble below to listen immediately, or click **Download MP3** for the full 320kbps tagged file.'}"
        )
        return {
            "success": True,
            "reply": reply,
            "mode": mode,
            "playlist_title": f"{t['title']} — {t['artist']}",
            "playlist_vibe": f"{t.get('album', 'Single')} ({t.get('year') or 'HD Audio'})",
            "tracks": [t]
        }

    p_title = f"{cleaned_query.title()} — Curated Mix"
    return {
        "success": True,
        "reply": f"I put together a **{cleaned_query.title()}** selection for you ({len(tracks)} tracks). Tap play on any track bubble to listen right here, download individual MP3s, or grab the 1-click Playlist ZIP!",
        "mode": "playlist",
        "playlist_title": p_title,
        "playlist_vibe": f"Curated for \"{cleaned_query}\" • 320kbps & ID3 Tagged",
        "tracks": tracks
    }


@app.route('/api/assistant/chat', methods=['POST'])
def assistant_chat():
    data = request.json or {}
    user_message = data.get('message', '').strip()
    history = data.get('history', [])
    options = data.get('options', {})
    result = curate_music_response(user_message, history=history, options=options)
    status_code = 200 if result.get("success") else 400
    return jsonify(result), status_code


@app.route('/api/assistant/prepare_download', methods=['POST'])
def assistant_prepare_download():
    """
    Queues a ConversionJob from one or more curated chat tracks so the user can download
    a single tagged MP3/M4R or a complete playlist ZIP directly inside the chat thread.
    """
    user = get_or_create_user()
    data = request.json or {}
    tracks = data.get('tracks', [])
    if not tracks or not isinstance(tracks, list):
        return jsonify({"error": "No tracks provided for download."}), 400

    session_id = data.get('session_id', str(uuid.uuid4()))
    increase_quality = bool(data.get('increase_quality', False))
    auto_add_album_art = bool(data.get('auto_add_album_art', True))
    transcribe_audio = bool(data.get('transcribe_audio', False))
    is_ringtone = bool(data.get('is_ringtone', False))
    start_time = data.get('start_time')
    end_time = data.get('end_time')

    valid_entries = []
    for idx, t in enumerate(tracks[:MAX_SONGS]):
        t_title = (t.get('title') or f"Track {idx+1}").strip()
        t_artist = (t.get('artist') or "Unknown Artist").strip()
        t_thumb = (t.get('artwork_url') or "").strip()
        t_query = (t.get('search_query') or t.get('url') or f"ytsearch1:{t_artist} - {t_title} official audio").strip()
        valid_entries.append((idx + 1, t_query, t_title, t_artist, t_thumb))

    total_tracks = len(valid_entries)
    total_credits_needed = max(0, total_tracks - 5) * 1
    if auto_add_album_art:
        total_credits_needed += max(0, total_tracks - 5) * 1
    if increase_quality:
        total_credits_needed += total_tracks * 1
    if transcribe_audio:
        total_credits_needed += total_tracks * 10

    is_premium_job = transcribe_audio or increase_quality

    payment_method = None
    if not is_premium_job and total_tracks <= 5:
        payment_method = 'always_free'
    elif getattr(user, 'subscription_active', False):
        payment_method = 'subscription'
    elif user.paid_track_credits >= total_credits_needed:
        user.paid_track_credits -= total_credits_needed
        payment_method = 'credits'
    else:
        return jsonify({
            "error": f"This download requires {total_credits_needed} credits. Free tier includes up to 5 standard tracks per request—toggle off paid add-ons or add credits!",
            "requires_payment": True,
            "credits_needed": total_credits_needed
        }), 403

    # Record in PopularURL so community activity feed stays fresh
    first_t = tracks[0]
    playlist_title = data.get('playlist_title') or f"{first_t.get('title', 'Track')} - {first_t.get('artist', '')}"
    try:
        synthetic_url = first_t.get('search_query') or f"chat://{uuid.uuid4().hex[:8]}"
        pop = PopularURL.query.filter_by(url=synthetic_url[:500]).first()
        if pop:
            pop.conversion_count += 1
            pop.last_converted = datetime.utcnow()
        else:
            pop = PopularURL(
                url=synthetic_url[:500],
                title=playlist_title[:200],
                artist=(first_t.get('artist') or 'MP3aud.io Assistant')[:200],
                thumbnail_url=(first_t.get('artwork_url') or '')[:500]
            )
            db.session.add(pop)
    except Exception:
        db.session.rollback()

    queue_position = ConversionJob.query.filter_by(status='queued').count() + 1
    job_priority = 1 if payment_method in ('credits', 'subscription') else 0

    new_job = ConversionJob(
        id=session_id,
        user_id=user.id,
        payment_method=payment_method,
        status='queued',
        priority=job_priority,
        total=total_tracks,
        entries=valid_entries,
        url=playlist_title[:500],
        user_email=user.email if not user.email.startswith('anon_') else None,
        start_time=start_time,
        end_time=end_time,
        transcribe_audio=transcribe_audio,
        increase_quality=increase_quality,
        organize_genre=False,
        auto_add_album_art=auto_add_album_art,
        video_to_mp3=False,
        is_ringtone=is_ringtone
    )
    db.session.add(new_job)
    db.session.commit()

    return jsonify({
        "session_id": session_id,
        "total_tracks": total_tracks,
        "status": "queued",
        "queue_position": queue_position,
        "single_mp3_url": f"/download_mp3/{session_id}",
        "zip_url": f"/download/{session_id}/playlist_backup.zip"
    }), 200


@app.route('/download_mp3/<session_id>', methods=['GET'])
def download_single_mp3(session_id):
    """
    Extracts and serves the individual .mp3 (or .m4r if format=m4r) directly from the job's archive
    so users clicking 'Download MP3' on a single track bubble get a direct .mp3 file!
    """
    session_id = secure_filename(session_id)
    want_ext = request.args.get('ext', 'mp3').lower()
    if want_ext not in ('mp3', 'm4r'):
        want_ext = 'mp3'

    zip_bytes_io = None
    local_zip = os.path.join(DOWNLOAD_FOLDER, session_id, "playlist_backup.zip")
    if os.path.exists(local_zip):
        zip_bytes_io = local_zip
    elif s3_client and S3_BUCKET:
        try:
            s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"downloads/{session_id}/playlist_backup.zip")
            zip_bytes_io = io.BytesIO(s3_obj['Body'].read())
        except Exception as e:
            logger.error(f"Could not fetch zip from S3 for single MP3 extraction: {e}")

    if not zip_bytes_io:
        return "File not found or expired", 404

    try:
        with zipfile.ZipFile(zip_bytes_io, 'r') as z:
            names = z.namelist()
            target_member = None
            for name in names:
                if name.lower().endswith(f".{want_ext}"):
                    target_member = name
                    break
            if not target_member and names:
                target_member = names[0]
            if not target_member:
                return "Archive is empty", 404

            file_data = z.read(target_member)
            download_filename = os.path.basename(target_member)
            mime = 'audio/mp4' if download_filename.lower().endswith('.m4r') else 'audio/mpeg'
            return send_file(
                io.BytesIO(file_data),
                mimetype=mime,
                as_attachment=True,
                download_name=download_filename
            )
    except Exception as e:
        logger.error(f"Error extracting single MP3 from archive: {e}")
        return redirect(f"/download/{session_id}/playlist_backup.zip")


# ==============================================================================
# WHATSAPP & TELEGRAM BOT WEBHOOKS + CONFIGURATION ENDPOINTS
# ==============================================================================

@app.route('/api/assistant/bot-info', methods=['GET'])
def get_bot_info():
    tg_username = os.environ.get('TELEGRAM_BOT_USERNAME', 'MP3audioBot').lstrip('@')
    tg_token_set = bool(os.environ.get('TELEGRAM_BOT_TOKEN'))
    wa_number = os.environ.get('WHATSAPP_NUMBER', '').strip()
    wa_clean = re.sub(r'[^\d]', '', wa_number)
    wa_configured = bool(os.environ.get('WHATSAPP_ACCESS_TOKEN') or os.environ.get('TWILIO_AUTH_TOKEN') or wa_clean)

    base_api = PUBLIC_URL.rstrip('/')
    return jsonify({
        "telegram_bot_username": f"@{tg_username}",
        "telegram_url": f"https://t.me/{tg_username}",
        "telegram_configured": tg_token_set,
        "whatsapp_number": wa_number or "+1 (555) MP3-AUDIO",
        "whatsapp_url": f"https://wa.me/{wa_clean}?text=Send%20me%20a%20chill%20late-night%20synthwave%20playlist" if wa_clean else "https://wa.me/?text=Check%20out%20MP3aud.io%20Music%20Assistant",
        "whatsapp_configured": wa_configured,
        "webhook_telegram": f"{base_api}/webhook/telegram",
        "webhook_whatsapp": f"{base_api}/webhook/whatsapp"
    }), 200


@app.route('/api/bot/setup-telegram', methods=['POST'])
def setup_telegram_webhook():
    """Registers the Telegram Bot Webhook URL with Telegram's Bot API."""
    data = request.json or {}
    bot_token = (data.get('bot_token') or os.environ.get('TELEGRAM_BOT_TOKEN') or '').strip()
    if not bot_token:
        return jsonify({"error": "TELEGRAM_BOT_TOKEN is required (pass in body or set in environment variables)."}), 400

    webhook_url = (data.get('webhook_url') or f"{PUBLIC_URL.rstrip('/')}/webhook/telegram").strip()
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{bot_token}/setWebhook",
            json={"url": webhook_url, "allowed_updates": ["message", "callback_query"]},
            timeout=10
        )
        return jsonify(resp.json()), resp.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def handle_telegram_message_async(chat_id, text_msg, bot_token):
    """Processes a user's Telegram text message and sends back playable audio bubbles + download links."""
    api_base = f"https://api.telegram.org/bot{bot_token}"
    try:
        if text_msg.strip().lower() in ('/start', '/help', 'hi', 'hello'):
            welcome = (
                "🎧 *Welcome to MP3aud.io Personal Music Assistant!*\n\n"
                "Text me anything you want to hear and I'll send playable tracks & playlists right into this chat:\n"
                "• *Any Song or Artist:* _\"Blinding Lights by The Weeknd\"_\n"
                "• *Any Vibe or Playlist:* _\"Send me a chill late-night synthwave playlist\"_\n"
                "• *Workout / Study Mixes:* _\"90s hip hop workout mix\"_\n"
                "• *Video/Music Links:* Paste any YouTube, SoundCloud, or TikTok link to extract 320kbps MP3s!\n\n"
                "What are you in the mood to listen to?"
            )
            requests.post(f"{api_base}/sendMessage", json={
                "chat_id": chat_id,
                "text": welcome,
                "parse_mode": "Markdown"
            }, timeout=10)
            return

        # Show 'upload_audio' status indicator in Telegram
        requests.post(f"{api_base}/sendChatAction", json={"chat_id": chat_id, "action": "upload_audio"}, timeout=5)

        curated = curate_music_response(text_msg)
        tracks = curated.get("tracks", [])
        reply_text = curated.get("reply", "Here are your curated tracks!")
        playlist_title = curated.get("playlist_title", "MP3aud.io Mix")

        # Queue a background ConversionJob so the user also gets a 1-click full MP3/ZIP download link
        session_id = str(uuid.uuid4())
        with app.app_context():
            valid_entries = [
                (idx + 1, t.get('search_query', f"ytsearch1:{t['artist']} - {t['title']} audio"), t['title'], t['artist'], t.get('artwork_url', ''))
                for idx, t in enumerate(tracks[:6])
            ]
            if valid_entries:
                job = ConversionJob(
                    id=session_id,
                    user_id=None,
                    payment_method='always_free',
                    status='queued',
                    priority=0,
                    total=len(valid_entries),
                    entries=valid_entries,
                    url=playlist_title[:500],
                    auto_add_album_art=True
                )
                db.session.add(job)
                db.session.commit()

        web_chat_url = f"{FRONTEND_URL.rstrip('/')}/?q={urllib.parse.quote(text_msg)}"
        zip_download_url = f"{PUBLIC_URL.rstrip('/')}/download/{session_id}/playlist_backup.zip"
        single_mp3_url = f"{PUBLIC_URL.rstrip('/')}/download_mp3/{session_id}"

        track_lines = "\n".join([f"{t['index']}. *{t['title']}* — {t['artist']} ({t['duration']})" for t in tracks[:6]])
        summary_msg = f"🎵 *{playlist_title}*\n{reply_text}\n\n{track_lines}"

        inline_kb = [
            [{"text": "🎧 Open Interactive Web Player", "url": web_chat_url}],
            [{"text": "⬇️ Download Full MP3" if len(tracks) == 1 else "📦 Download Full Playlist (.ZIP)", "url": single_mp3_url if len(tracks) == 1 else zip_download_url}]
        ]

        requests.post(f"{api_base}/sendMessage", json={
            "chat_id": chat_id,
            "text": summary_msg,
            "parse_mode": "Markdown",
            "reply_markup": {"inline_keyboard": inline_kb}
        }, timeout=10)

        # Send direct playable audio bubbles in Telegram for each track that has a preview stream
        for t in tracks[:5]:
            preview_url = t.get("preview_url")
            if preview_url:
                requests.post(f"{api_base}/sendAudio", json={
                    "chat_id": chat_id,
                    "audio": preview_url,
                    "title": t.get("title", "Track"),
                    "performer": t.get("artist", "Artist"),
                    "thumbnail": t.get("artwork_url", ""),
                    "caption": f"🎵 {t.get('title')} — {t.get('artist')}\n💿 {t.get('album', '')}"
                }, timeout=12)
    except Exception as e:
        logger.error(f"Telegram bot async handler error: {e}")


@app.route('/webhook/telegram', methods=['POST'])
def telegram_webhook():
    """Incoming webhook for Telegram Bot messages."""
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN', '').strip()
    payload = request.json or {}
    message = payload.get("message") or payload.get("edited_message")
    if message and bot_token:
        chat_id = message.get("chat", {}).get("id")
        text_msg = message.get("text") or message.get("caption") or ""
        if chat_id and text_msg:
            Thread(target=handle_telegram_message_async, args=(chat_id, text_msg, bot_token), daemon=True).start()
    return jsonify({"ok": True}), 200


@app.route('/webhook/whatsapp', methods=['GET', 'POST'])
def whatsapp_webhook():
    """
    Supports both:
    1) Meta WhatsApp Cloud API (GET verification & POST JSON messages)
    2) Twilio WhatsApp / SMS Messaging Webhook (POST form-urlencoded From & Body -> TwiML reply)
    """
    if request.method == 'GET':
        mode = request.args.get('hub.mode')
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        verify_token = os.environ.get('WHATSAPP_VERIFY_TOKEN', 'mp3audio_verify_token')
        if mode == 'subscribe' and token == verify_token:
            return challenge, 200
        return "Verification token mismatch", 403

    # Check if this is a Twilio WhatsApp/SMS form webhook
    if request.form and ('Body' in request.form or 'From' in request.form):
        incoming_msg = request.form.get('Body', '').strip()
        if not incoming_msg:
            incoming_msg = "chill playlist"

        curated = curate_music_response(incoming_msg)
        tracks = curated.get("tracks", [])
        playlist_title = curated.get("playlist_title", "MP3aud.io Mix")

        # Queue conversion job for 1-click download
        session_id = str(uuid.uuid4())
        valid_entries = [
            (idx + 1, t.get('search_query', f"ytsearch1:{t['artist']} - {t['title']} audio"), t['title'], t['artist'], t.get('artwork_url', ''))
            for idx, t in enumerate(tracks[:6])
        ]
        if valid_entries:
            job = ConversionJob(
                id=session_id,
                user_id=None,
                payment_method='always_free',
                status='queued',
                priority=0,
                total=len(valid_entries),
                entries=valid_entries,
                url=playlist_title[:500],
                auto_add_album_art=True
            )
            db.session.add(job)
            db.session.commit()

        dl_link = f"{PUBLIC_URL.rstrip('/')}/download_mp3/{session_id}" if len(tracks) == 1 else f"{PUBLIC_URL.rstrip('/')}/download/{session_id}/playlist_backup.zip"
        web_link = f"{FRONTEND_URL.rstrip('/')}/?q={urllib.parse.quote(incoming_msg)}"
        track_lines = "\n".join([f"{t['index']}. {t['title']} - {t['artist']}" for t in tracks[:5]])
        body_text = (
            f"🎧 {playlist_title}\n\n"
            f"{track_lines}\n\n"
            f"▶️ Play & Chat: {web_link}\n"
            f"⬇️ Download MP3/ZIP: {dl_link}"
        )
        media_tag = ""
        if tracks and tracks[0].get("preview_url"):
            media_tag = f"<Media>{tracks[0]['preview_url']}</Media>"
        elif tracks and tracks[0].get("artwork_url"):
            media_tag = f"<Media>{tracks[0]['artwork_url']}</Media>"

        twiml = f'<?xml version="1.0" encoding="UTF-8"?><Response><Message><Body>{body_text}</Body>{media_tag}</Message></Response>'
        return app.response_class(twiml, mimetype='text/xml'), 200

    # Otherwise handle Meta WhatsApp Cloud API JSON payload
    payload = request.json or {}
    wa_token = os.environ.get('WHATSAPP_ACCESS_TOKEN', '').strip()
    phone_number_id = os.environ.get('WHATSAPP_PHONE_NUMBER_ID', '').strip()
    try:
        entries = payload.get("entry", [])
        for entry in entries:
            for change in entry.get("changes", []):
                value = change.get("value", {})
                messages = value.get("messages", [])
                for msg_obj in messages:
                    from_number = msg_obj.get("from")
                    text_body = (msg_obj.get("text") or {}).get("body", "").strip()
                    if from_number and text_body and wa_token and phone_number_id:
                        curated = curate_music_response(text_body)
                        tracks = curated.get("tracks", [])
                        playlist_title = curated.get("playlist_title", "MP3aud.io Mix")
                        web_link = f"{FRONTEND_URL.rstrip('/')}/?q={urllib.parse.quote(text_body)}"
                        track_lines = "\n".join([f"{t['index']}. *{t['title']}* — {t['artist']}" for t in tracks[:5]])
                        reply_body = f"🎧 *{playlist_title}*\n{curated.get('reply', '')}\n\n{track_lines}\n\n▶️ *Listen & Download MP3/ZIP:* {web_link}"

                        graph_url = f"https://graph.facebook.com/v19.0/{phone_number_id}/messages"
                        headers = {"Authorization": f"Bearer {wa_token}", "Content-Type": "application/json"}
                        requests.post(graph_url, headers=headers, json={
                            "messaging_product": "whatsapp",
                            "to": from_number,
                            "type": "text",
                            "text": {"preview_url": True, "body": reply_body}
                        }, timeout=10)

                        # Also send playable audio messages directly in WhatsApp
                        for t in tracks[:3]:
                            if t.get("preview_url"):
                                requests.post(graph_url, headers=headers, json={
                                    "messaging_product": "whatsapp",
                                    "to": from_number,
                                    "type": "audio",
                                    "audio": {"link": t["preview_url"]}
                                }, timeout=10)
    except Exception as e:
        logger.error(f"Meta WhatsApp webhook error: {e}")

    return jsonify({"status": "received"}), 200


if __name__ == '__main__': app.run(debug=False, port=5000, threaded=True)