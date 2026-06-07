import os, uuid, logging, glob, zipfile, certifi, gc, shutil, time, subprocess, math, tempfile, hmac, hashlib
from datetime import datetime, timedelta
from flask import Flask, request, send_file, jsonify, session, redirect, url_for
from werkzeug.utils import secure_filename
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
from yt_dlp import YoutubeDL
import json
import requests
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
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-prod')
app.config['SESSION_COOKIE_SAMESITE'] = 'None'
app.config['SESSION_COOKIE_SECURE'] = True

# Render Postgres Compatibility Fix
db_url = os.environ.get('DATABASE_URL', 'sqlite:///mp3audio.db')
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# For production security, it's best to lock down CORS to your specific frontend URL.
# This reads the URL from the same environment variable used for magic links.
frontend_url = os.environ.get('FRONTEND_URL', 'https://mp3aud.io').rstrip('/')
allowed_origins = [
    frontend_url,
    "https://www.mp3aud.io",
    "http://localhost:3000",
    "http://127.0.0.1:5500"
]
CORS(app, supports_credentials=True, resources={
    r"/*": { "origins": allowed_origins, "methods": ["GET", "POST", "OPTIONS"], "allow_headers": ["Content-Type", "Authorization", "X-Admin-Secret"] }
})

db = SQLAlchemy(app)
serializer = URLSafeTimedSerializer(app.config['SECRET_KEY'])

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    free_conversions_used = db.Column(db.Integer, default=0)
    paid_track_credits = db.Column(db.Integer, default=0)

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

class PopularURL(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String(500), unique=True, nullable=False)
    title = db.Column(db.String(200))
    artist = db.Column(db.String(200))
    conversion_count = db.Column(db.Integer, default=1)
    last_converted = db.Column(db.DateTime, default=datetime.utcnow)

def initialize_database():
    """Runs database setup in the background to prevent boot stalling."""
    with app.app_context():
        try:
            db.create_all()
            
            # SYSTEM REBOOT RECOVERY: Refund credits for jobs interrupted by a sudden crash
            zombie_jobs = ConversionJob.query.filter(ConversionJob.status.in_(['queued', 'processing'])).all()
            if zombie_jobs:
                for z_job in zombie_jobs:
                    user = User.query.get(z_job.user_id) if z_job.user_id else None
                    unused_tracks = z_job.total - z_job.completed
                    if user and unused_tracks > 0:
                        if z_job.payment_method == 'credits':
                            user.paid_track_credits += unused_tracks
                        elif z_job.payment_method == 'free':
                            user.free_conversions_used = max(0, user.free_conversions_used - unused_tracks)
                    z_job.status = 'error'
                    z_job.error = 'Job interrupted by server reboot.'
                db.session.commit()
                logger.warning(f"Recovered and refunded {len(zombie_jobs)} jobs interrupted by server reboot.")
        except Exception as e:
            logger.error(f"Database initialization delayed or failed: {e}")

# Trigger DB setup safely without blocking Gunicorn
Thread(target=initialize_database, daemon=True).start()

DOWNLOAD_FOLDER = os.path.join(os.getcwd(), 'downloads')
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
try: client = OpenAI()
except: client = None

MAX_SONGS = 350
AVG_TIME_PER_TRACK = 45  
PUBLIC_URL = os.environ.get('PUBLIC_URL', 'https://mp3aud.io')
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'https://mp3aud.io')

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
                unused_tracks = job.total - job.completed
                if unused_tracks > 0:
                    refund_unused_credits(job.user_id, job.payment_method, unused_tracks)
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
    try:
        resend.api_key = os.environ.get('RESEND_API_KEY')
        resend.Emails.send({
            "from": f"MP3 Audio Tools <{os.environ.get('FROM_EMAIL')}>",
            "to": [recipient],
            "subject": subject,
            "html": html_content,
        })
    except: pass

def get_or_create_user():
    if 'user_id' in session:
        user = User.query.get(session['user_id'])
        if user: return user
        
    fake_email = f"anon_{uuid.uuid4().hex[:12]}@guest.local"
    ghost_user = User(email=fake_email)
    db.session.add(ghost_user)
    db.session.commit()
    session['user_id'] = ghost_user.id
    return ghost_user

def refund_unused_credits(user_id, payment_method, unused_tracks, session_id=None):
    try:
        with app.app_context():
            if unused_tracks > 0 and user_id and payment_method:
                user = User.query.get(user_id)
                if user:
                    if payment_method == 'credits':
                        user.paid_track_credits += unused_tracks
                    elif payment_method == 'free':
                        user.free_conversions_used = max(0, user.free_conversions_used - unused_tracks)
            db.session.commit()
    except Exception as e:
        logger.error(f"Failed to refund credits: {e}")

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
    magic_url = f"{FRONTEND_URL}?token={token}"
    
    email_subject = "Secure Login - MP3aud.io"
    html = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 30px; max-width: 600px; margin: 0 auto; border: 2px solid #fde68a; border-radius: 12px; background-color: #fffbeb;">
        <h2 style="margin: 0 0 15px 0; color: #92400e; font-size: 22px; font-weight: 800;">Log In & Manage Credits</h2>
        <p style="color: #92400e; font-size: 15px; margin-bottom: 24px; line-height: 1.5;">Click the button below to securely log in to your account. Once inside, you can purchase more credits or check your existing balance.</p>
        <a href="{magic_url}" style="background-color: #ea580c; color: white; padding: 12px 24px; text-decoration: none; border-radius: 8px; display: inline-block; font-weight: bold; font-size: 16px;">Log In Now</a>
        <p style="color: #b45309; font-size: 12px; margin-top: 25px; line-height: 1.4;">If you didn't request this link, you can safely ignore this email. The link will expire in 1 hour.</p>
    </div>"""
    send_email_notification(email, email_subject, html)
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
        return jsonify({"success": True})
    return jsonify({"error": "User not found"}), 404

@app.route('/auth/me', methods=['GET'])
def get_current_user():
    user = get_or_create_user()
    is_guest = user.email.startswith('anon_')
    return jsonify({
        "authenticated": not is_guest,
        "email": None if is_guest else user.email,
        "free_conversions_used": user.free_conversions_used,
        "paid_track_credits": user.paid_track_credits
    })

@app.route('/auth/logout', methods=['POST'])
def logout():
    session.pop('user_id', None)
    return jsonify({"success": True})

@app.route('/buy-credits', methods=['POST'])
def generate_invoice():
    user = get_or_create_user()
    if user.email.startswith('anon_'):
        return jsonify({"error": "Unauthorized. Please log in first."}), 401
    payload = {
        "price_amount": 5.00,
        "price_currency": "usd",
        "order_id": str(user.id), 
        "order_description": "350 Track Conversions",
        "ipn_callback_url": f"{PUBLIC_URL.rstrip('/')}/webhook/nowpayments"
    }
    try:
        headers = {'x-api-key': os.environ.get('NOWPAYMENTS_API_KEY'), 'Content-Type': 'application/json'}
        response = requests.post('https://api.nowpayments.io/v1/invoice', headers=headers, json=payload)
        if response.status_code == 200: return jsonify({"invoice_url": response.json().get('invoice_url')})
        return jsonify({"error": "Failed to connect to payment gateway."}), 500
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/webhook/nowpayments', methods=['POST'])
def nowpayments_webhook():
    secret_key = os.environ.get('NOWPAYMENTS_IPN_SECRET', '').encode('utf-8')
    if request.headers.get('x-nowpayments-sig') != hmac.new(secret_key, request.get_data(), hashlib.sha512).hexdigest():
        return jsonify({"error": "Invalid Signature"}), 403
        
    data = request.json
    
    if data and data.get('payment_status') == 'finished':
        try:
            price_amount = float(data.get('price_amount', 0))
            price_currency = data.get('price_currency', '').lower()
            
            if price_amount == 5.00 and price_currency == 'usd':
                user = User.query.get(int(data.get('order_id')))
                if user:
                    user.paid_track_credits += 350
                    db.session.commit()
            else:
                logger.warning(f"Payment amount mismatch: Expected $5.00 usd, got {price_amount} {price_currency} for order {data.get('order_id')}")
                
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing payment amount: {e}")
            
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

def process_track(url, session_dir, track_index, ffmpeg_exe, session_id, zip_path, track_name, artist_name, thumbnail, start_time, end_time, transcribe_audio):
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
            {'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '128'},
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

    try:
        job.current_track = track_index
        job.last_update = time.time()
        job.current_status = f'Initializing track {track_index}...'
        job.sub_progress = 0
        job.current_thumbnail = thumbnail 
        db.session.commit()
        
        if job.status == 'cancelled': return False

        try:
            with YoutubeDL({'quiet':True, 'no_warnings':True, 'socket_timeout':10}) as ydl:
                info = ydl.extract_info(url, download=False)
                if info.get('title'): track_name = info['title']
                if info.get('uploader'): artist_name = info['uploader']
                if info.get('thumbnail'): job.current_thumbnail = info['thumbnail']
                db.session.commit()
        except: pass
        
        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        
        mp3_files = glob.glob(os.path.join(session_dir, f"{temp_filename_base}*.mp3"))
        if mp3_files:
            file_to_zip = mp3_files[0]

            try:
                cmd = [ffmpeg_exe, '-y', '-i', file_to_zip]
                
                # -map 0 ensures the yt-dlp embedded thumbnail is copied over with the audio
                cmd.extend(['-map', '0', '-c', 'copy'])
                    
                cmd.extend(['-metadata', f'title={track_name}', '-metadata', f'artist={artist_name}', file_to_zip + '.tmp'])
                
                subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
                if os.path.exists(file_to_zip + '.tmp'): 
                    os.replace(file_to_zip + '.tmp', file_to_zip)
            except: pass

            clean_name = "".join([c for c in f"{artist_name} - {track_name}"[:100] if c.isalnum() or c in (' ', '-', '_')]).strip() or f"Track_{track_index}"
            
            with zipfile.ZipFile(zip_path, 'a', zipfile.ZIP_STORED) as z:
                z.write(file_to_zip, f"{clean_name}.mp3")
            
            if transcribe_audio:
                raw_txt_path, raw_pdf_path = transcribe_audio_file(file_to_zip, job)
                
                if raw_txt_path:
                    html_path, summary_pdf_path, manual_html = generate_diy_manual(raw_txt_path, job)
                    
                    with zipfile.ZipFile(zip_path, 'a', zipfile.ZIP_STORED) as z:
                        if raw_pdf_path and os.path.exists(raw_pdf_path): z.write(raw_pdf_path, f"{clean_name}_raw_transcript.pdf")
                        if summary_pdf_path and os.path.exists(summary_pdf_path): z.write(summary_pdf_path, f"{clean_name}_summary.pdf")

                    if manual_html: job.email_summaries = (job.email_summaries or "") + f"<hr><h2>{clean_name}</h2>" + manual_html

            job.completed += 1
            job.sub_progress = 100
            
            completed_list = list(job.completed_tracks)
            completed_list.append(clean_name)
            job.completed_tracks = completed_list
            db.session.commit()
            
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
        if job.status != 'cancelled': 
            job.skipped += 1
            error_string = str(e)
            
            if "404" in error_string:
                friendly_reason = "Private, deleted, or invalid track link."
            elif "403" in error_string:
                friendly_reason = "Geo-blocked or access denied by platform."
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
                
                process_track(t_url, session_dir, idx, ffmpeg_exe, session_id, zip_path, t_title, t_artist, t_thumb, job.start_time, job.end_time, job.transcribe_audio)

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
            job.status = 'error'
            job.error = str(e)
            db.session.commit()
        finally:
            job = ConversionJob.query.get(session_id)
            if job:
                unused_tracks = job.total - job.completed
                refund_unused_credits(job.user_id, job.payment_method, unused_tracks, session_id)
            
            cleanup_memory()

def worker_loop():
    while True:
        try:
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
                    run_conversion_task(session_id)
                else:
                    time.sleep(1)
        except Exception as e: 
            logger.error(f"Worker queue error: {e}")
            time.sleep(1)

queue_worker = Thread(target=worker_loop, daemon=True)
queue_worker.start()

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
            
    except Exception as e:
        valid_entries = [(1, url, "Unknown Track", "Unknown Artist", "")]
        total_tracks = 1
        
    payment_method = None
    
    # 1. If the playlist is 5 tracks or fewer, it's always free!
    if total_tracks <= 5:
        payment_method = 'free'
        user.free_conversions_used += total_tracks
        
    # 2. If it's larger than 5 tracks, check if they have enough paid credits
    elif user.paid_track_credits >= total_tracks:
        user.paid_track_credits -= total_tracks
        payment_method = 'credits'
        
    # 3. If it's larger than 5 tracks AND they don't have enough credits, prompt payment
    else:
        return jsonify({
            "error": f"Playlists larger than 5 tracks require credits. This playlist has {total_tracks} tracks, but you only have {user.paid_track_credits} credits.", 
            "requires_payment": True
        }), 403

    
    # --- TRACK POPULAR URL ---

    popular_url = PopularURL.query.filter_by(url=url).first()
    if popular_url:
        popular_url.conversion_count += 1
        popular_url.last_converted = datetime.utcnow()
    else:
        popular_url = PopularURL(
            url=url, 
            title=playlist_title[:200] if playlist_title else 'Audio URL', 
            artist=playlist_artist[:200] if playlist_artist else ''
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
        transcribe_audio=data.get('transcribe_audio', False)
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
        wait_seconds = ahead_count * AVG_TIME_PER_TRACK * 5 # Approx 5 tracks avg

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
        
        unused_tracks = job.total - job.completed
        if unused_tracks > 0:
            refund_unused_credits(job.user_id, job.payment_method, unused_tracks, session_id=None)
            
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
        # Get the top most converted URLs, break ties by newest conversion date
        top_urls = PopularURL.query.order_by(PopularURL.conversion_count.desc(), PopularURL.last_converted.desc()).limit(limit).offset(offset).all()
        result = []
        for item in top_urls:
            result.append({
                "title": item.title or "Unknown Title",
                "desc": item.artist or "Various Artists",
                "link": item.url,
                "date": item.last_converted.strftime("%b %d, %Y"),
                "count": item.conversion_count
            })
        return jsonify({"success": True, "data": result}), 200
    except Exception as e:
        logger.error(f"Error fetching top urls: {e}")
        # Fallback gracefully to an empty list so the frontend doesn't throw a red error
        # This handles cases where the DB table is still initializing
        return jsonify({"success": True, "data": [], "error": str(e)}), 200

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
def health(): return jsonify({"status": "ok"}), 200
@app.route('/')
def index(): return jsonify({"message": "Audio Processor API", "status": "active"}), 200

if __name__ == '__main__': app.run(debug=False, port=5000, threaded=True)