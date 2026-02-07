import logging
import os
import random
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re

import jinja2
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

from utils.recommendation_weights import (
    get_known_artists, 
    get_intelligent_feedback_weights,
    calculate_raw_score_with_precomputed_weights,
    get_psychologically_adjusted_percentage
)

import base64

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

TABLE_NAME = "apple_music_album_releases"
VIEW_NAME = "v_weekly_new_releases"
SCHEMA = 'public'
VIEW_DATASET = Dataset("view://apple_music/v_weekly_new_releases")
BREVO_LOGIN = os.environ.get("BREVO_LOGIN")
BREVO_PASSWORD = os.environ.get("BREVO_PASSWORD")
TEST_MODE = False  

def generate_unsubscribe_token(user_id: int) -> str:
    """Simple token based on base64 encoding"""
    return base64.urlsafe_b64encode(str(user_id).encode()).decode()

def get_active_subscribers():
    """Fetches all active subscribers with their preferences."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    if TEST_MODE:
        query = """
            SELECT user_id, first_name, email, genres, favorite_artist, album_length, related_artists
            FROM user_preferences 
            WHERE is_active = TRUE AND email in ('nathanialc17@gmail.com')
        """
    else:
        query = """
            SELECT user_id, first_name, email, genres, favorite_artist, album_length, related_artists
            FROM user_preferences 
            WHERE is_active = TRUE
        """
    
    records = hook.get_records(query)
    
    # Convert to list of dictionaries
    subscribers = []
    for user_id, first_name, email, genres, favorite_artist, album_length, related_artists in records:
        # Clean the genres - remove quotes from JSON array
        clean_genres = []
        if genres and isinstance(genres, list):
            clean_genres = [str(g).strip('"') for g in genres]
        elif genres and isinstance(genres, str):
            clean_genres = [g.strip() for g in genres.split(',')]
        
        subscribers.append({
            'user_id': user_id,
            'first_name': first_name,
            'email': email,
            'genres': clean_genres,
            'favorite_artist': favorite_artist,
            'album_length': album_length,
            'related_artists': related_artists
        })
    
    return subscribers

# Need to find a more dynamic way to write blurbs, would be cool to hit openAI for this
def generate_album_blurb(artist: str, album: str) -> str:
    blurbs = [
        f"Fresh sounds from {artist} that you won't want to miss",
        f"{artist} returns with a compelling new collection",
        f"A standout release from {artist} worth checking out", 
        f"New music from {artist} that's making waves",
        f"{artist} delivers with this captivating new album",
        f"Don't miss this latest offering from {artist}",
        f"{artist} continues to innovate with this release",
        f"A must-listen addition from {artist}",
        f"This new album showcases {artist}'s evolving sound",
        f"{artist} brings the heat with this fresh collection"
    ]
    return random.choice(blurbs)

def fetch_this_weeks_albums():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    records = hook.get_records(f"""
        SELECT 
            artist, 
            album_name, 
            release_date, 
            cover_art_url, 
            genre,
            track_count, 
            COALESCE(editorial_notes, '') as notes,
            url
        FROM {SCHEMA}.{VIEW_NAME}
        ORDER BY release_date DESC
    """)
    
    # Convert to list of dictionaries with proper field names
    albums = []
    for record in records:
        #converting to protocol so it can jsut open up to apple music
        original_url = record[7]

        if original_url.startswith('https://music.apple.com'):
            app_url = original_url.replace('https://', 'music://', 1)
        else:
            app_url = original_url

        albums.append({
            'artist': record[0],
            'album_name': record[1],
            'release_date': record[2],
            'cover_art_url': record[3],
            'genre': record[4],
            'track_count': record[5],
            'notes': record[6],
            'url': app_url
        })
    
    return albums

def generate_email_content(**kwargs):
    run_date_raw = kwargs.get('ds')
    run_date = datetime.strptime(run_date_raw, "%Y-%m-%d").strftime("%A, %B %-d, %Y")
    logging.info(f"Generating personalized emails for run_date={run_date}")
    
    try:
        # Fetch all albums and subscribers
        all_albums = fetch_this_weeks_albums()
        subscribers = get_active_subscribers()
        
        # Fetch full known artist list once per run
        known_artists = get_known_artists()
        
        personalized_emails = {}
        
        for subscriber in subscribers:
            try:
                user_id = subscriber.get('user_id')
                user_stated_genres = subscriber.get('genres', [])
                artist_weights, genre_weights = get_intelligent_feedback_weights(user_id, user_stated_genres)
                
                # Calculate max possible score for this subscriber
                max_score = 100
                
                scored_albums = []
                for album in all_albums:
                    score = calculate_raw_score_with_precomputed_weights(
                        album, subscriber, known_artists, artist_weights, genre_weights
                    )
                    percentage_match = get_psychologically_adjusted_percentage(score, max_score)
                    
                    # Create a URL-safe album identifier
                    album_identifier = f"{album['artist']}|{album['album_name']}"
                    encoded_album_id = base64.urlsafe_b64encode(album_identifier.encode()).decode()
                    
                    scored_albums.append({
                        **album, 
                        'score': score,
                        'percentage_match': percentage_match,
                        'album_id': encoded_album_id,
                        'feedback_base': f"https://blurryblus.app/feedback?user={subscriber['user_id']}"
                    })
                
                scored_albums.sort(key=lambda x: x['score'], reverse=True)
                top_albums = scored_albums[:20]
                
                featured = top_albums[:3]
                others = top_albums[3:20]
                
                token = generate_unsubscribe_token(subscriber['user_id'])
                unsubscribe_url = f"https://blurryblus.app/unsubscribe/{token}"

                html = create_personalized_email_html(
                    subscriber,
                    featured,
                    others,
                    run_date,
                    unsubscribe_url=unsubscribe_url
                )
                personalized_emails[subscriber['email']] = html
                logging.info(f"Generated recommendations for {subscriber['email']}")
            except Exception as e:
                logging.error(f"Failed for {subscriber['email']}: {e}")
                continue
        
        ti = kwargs['ti']
        ti.xcom_push(key='personalized_emails', value=personalized_emails)
        return personalized_emails
        
    except Exception as e:
        logging.error(f"Failed to generate emails: {e}")
        raise

def get_match_color(percentage):
    """Return color based on match percentage"""
    if percentage >= 80:
        return "#10b981"  # Green
    elif percentage >= 60:
        return "#f59e0b"  # Amber
    elif percentage >= 40:
        return "#f97316"  # Orange
    else:
        return "#ef4444"  # Red

def create_personalized_email_html(subscriber, featured, others, run_date, unsubscribe_url):
    """HTML email with personalization details and genre info"""
    
    featured_with_blurbs = []
    for album in featured:
        blurb = generate_album_blurb(artist=album['artist'], album=album['album_name'])
        featured_with_blurbs.append({
            **album, 
            'blurb': blurb
        })

    # Split others into 4 rows of 4 albums each
    others_rows = [others[i:i+4] for i in range(0, min(len(others), 16), 4)]
    
    html = jinja2.Template("""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta name="x-apple-disable-message-reformatting">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <style>
            /* Reset for email clients */
            body, table, td, div, p { margin: 0; padding: 0; }
            body { font-family: Arial, sans-serif; background: #f8f9fa; color: #212529; }
            .container { max-width: 650px; margin: 0 auto; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
            
            /* Brand Header */
            .brand-header {
                text-align: center;
                padding: 10px 20px;
                background: #f1f5f9;
                border-bottom: 1px solid #e2e8f0;
            }
            .brand-name {
                font-weight: 800;
                color: #0f172a;
                font-size: 20px;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }
            .brand-tagline {
                color: #64748b;
                font-size: 13px;
                margin-top: 4px;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }
            
            /* Main Header */
            .header {
                text-align: center;
                padding: 30px 20px;
                /* Exact gradient from index.html */
                background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%);
                color: white;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }
            .header h1 {
                margin: 0;
                font-size: 28px;
                font-weight: 800;
                color: #ffffff;
            }
            .header p {
                margin: 8px 0 0;
                font-size: 16px;
                color: #e2e8f0;
            }
            .header-date {
                color: #cbd5e1;
                margin-top: 8px;
                font-size: 14px;
            }
            
            /* Content */
            .content { padding: 30px; }
            .section-title { 
                font-size: 22px; 
                font-weight: 600; 
                margin: 0 0 20px 0; 
                padding-bottom: 10px; 
                border-bottom: 2px solid #e9ecef; 
                color: #0f172a;  /* Your dark blue */
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }
            
            /* Featured section */
            .featured-section { margin-bottom: 30px; }
            .featured-table { width: 100%; border-spacing: 15px; border-collapse: separate; }
            .featured-cell { width: 33%; vertical-align: top; }
            .featured-album { 
                background: #f8f9fa; 
                border-radius: 12px; 
                overflow: hidden; 
                box-shadow: 0 4px 6px rgba(0,0,0,0.04); 
                position: relative;
                border-top: 3px solid #334155;  /* Brand blue accent */
            }
            .featured-cover { width: 100%; height: auto; display: block; }
            .featured-details { padding: 15px; }
            .featured-name { 
                font-weight: 700; 
                font-size: 16px; 
                margin: 0 0 5px 0; 
                color: #0f172a;  /* Your dark blue */
                line-height: 1.3; 
            }
            .featured-artist { 
                color: #475569;  /* Your accent blue */
                font-size: 14px; 
                margin: 0 0 10px 0; 
                font-weight: 500; 
            }
            .featured-blurb { 
                font-size: 13px; 
                color: #64748b;  /* Medium blue */
                font-style: italic; 
                margin: 0; 
                line-height: 1.4; 
            }
            
            /* Match percentage sticker - Blue theme */
            .match-sticker {
                position: absolute;
                top: 10px;
                right: 10px;
                background: rgba(255, 255, 255, 0.95);
                border-radius: 20px;
                padding: 6px 12px;
                font-size: 12px;
                font-weight: 700;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15);
                z-index: 10;
                backdrop-filter: blur(4px);
                border: 1px solid #e2e8f0;
            }
            .match-percentage {
                font-size: 13px;
                font-weight: 800;
            }
            
            /* Album info section */
            .album-info-section { 
                margin-top: 12px; 
                padding-top: 12px; 
                border-top: 1px solid #e2e8f0;  /* Your light border color */
            }
            .album-info-item { 
                font-size: 12px; 
                color: #64748b;  /* Medium blue */
                margin-bottom: 4px; 
            }
            .album-info-item strong {
                color: #334155;  /* Your medium blue */
            }
            
            /* Recommendations section */
            .recommendations-section { 
                background: #f8f9fa; 
                border-radius: 12px; 
                padding: 25px; 
                margin-top: 30px; 
                border: 1px solid #e2e8f0;  /* Matching your site */
            }
            .albums-table { width: 100%; border-spacing: 10px; border-collapse: separate; }
            .album-cell { width: 25% !important; vertical-align: top; }  /* ADDED !important */
            .album-card { 
                background: #ffffff; 
                border-radius: 8px; 
                overflow: hidden; 
                box-shadow: 0 2px 4px rgba(0,0,0,0.04); 
                position: relative;
                border: 1px solid #e2e8f0;  /* Light border matching your site */
            }
            .album-cover { width: 100%; height: auto; display: block; }
            .album-info { padding: 10px 8px; }
            .album-name { 
                font-weight: 600; 
                font-size: 13px; 
                margin: 0 0 4px 0; 
                line-height: 1.3; 
                color: #0f172a;  /* Your dark blue */
            }
            .album-artist { 
                color: #475569;  /* Your accent blue */
                font-size: 12px; 
                margin: 0; 
            }
            
            /* Small match indicator for recommendation cards */
            .small-match {
                position: absolute;
                top: 6px;
                right: 6px;
                background: rgba(255, 255, 255, 0.95);
                border-radius: 12px;
                padding: 3px 8px;
                font-size: 10px;
                font-weight: 700;
                box-shadow: 0 1px 4px rgba(0,0,0,0.1);
                z-index: 10;
                border: 1px solid #e2e8f0;
            }
            
            .feedback-buttons {
            text-align: center;
            margin: 12px 0 8px 0;
            }
            .feedback-btn {
                display: inline-block;
                border-radius: 4px;
                padding: 6px 12px;
                text-decoration: none;
                font-size: 14px;
                font-weight: 500;
                margin: 0 4px;
                transition: all 0.2s ease;
            }
            .btn-up {
                background: #e2e8f0;
                color: #334155;
                border: 1px solid #cbd5e1;
            }
            .btn-up:hover {
                background: #cbd5e1;
                border-color: #94a3b8;
                color: #334155;
            }
            .btn-down {
                background: #e2e8f0;
                color: #334155;
                border: 1px solid #cbd5e1;
            }
            .btn-down:hover {
                background: #cbd5e1;
                border-color: #94a3b8;
                color: #334155;
            }
            
            /* Footer */
            .footer { 
                text-align: center; 
                padding: 25px; 
                background: #0f172a;  /* Your dark blue */
                color: #cbd5e1;  /* Your text-gray color */
                font-size: 14px; 
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }
            .footer p { 
                margin: 5px 0; 
                color: #cbd5e1;
            }
            .footer a {
                color: #94a3b8;  /* Lighter blue for links */
                text-decoration: none;
            }
            .footer a:hover {
                color: #e2e8f0;
                text-decoration: underline;
            }
            .signature {
                color: #cbd5e1;
                font-style: italic;
                margin-top: 8px;
            }
            
            /* Mobile styles */
            @media only screen and (max-width: 650px) {
                .container { border-radius: 0; box-shadow: none; }
                .featured-cell, .album-cell { display: block; width: 100% !important; margin-bottom: 15px; }
                .featured-table, .albums-table { border-spacing: 0 !important; }
                .content { padding: 20px !important; }
                .header { padding: 20px 15px !important; }
                .header h1 { font-size: 24px !important; }
                .recommendations-section { padding: 20px !important; }
                .section-title { font-size: 20px !important; }
                .match-sticker { top: 8px; right: 8px; padding: 4px 10px; font-size: 11px; }
                .small-match { top: 4px; right: 4px; padding: 2px 6px; font-size: 9px; }
                .feedback-btn { padding: 8px 16px; margin: 0 8px 8px 0; }
                .brand-header { padding: 8px 15px; }
                .brand-name { font-size: 18px; }
            }
            
            @media only screen and (max-width: 480px) {
                .header h1 { font-size: 22px !important; }
                .header p { font-size: 14px !important; }
                .section-title { font-size: 18px !important; }
            }
            
            /* Safari-specific fixes */
            @media screen and (-webkit-min-device-pixel-ratio: 0) {
                .container, table, td, div, p {
                    -webkit-text-size-adjust: 100% !important;
                    text-size-adjust: 100% !important;
                }
            }
        </style>
    </head>
    <body>
        <center class="container">
            <!-- Brand Header -->
            <table width="100%" cellpadding="0" cellspacing="0" border="0" class="brand-header">
                <tr>
                    <td>
                        <div class="brand-name">BlurryBlus</div>
                        <div class="brand-tagline">Personalized Music Discovery</div>
                    </td>
                </tr>
            </table>
            
            <!-- Main Content -->
            <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <!-- Hero Header -->
                <tr>
                    <td class="header" style="background: #0f172a; color: white;">
                        <h1 style="color: #ffffff; margin: 0; font-size: 28px; font-weight: 800;">Hi {{ first_name }}, Here's Your Weekly Music Discovery</h1>
                        <p style="color: #e2e8f0; margin: 8px 0 0; font-size: 16px;">Curated just for you based on your music taste</p>
                        <p class="header-date" style="color: #cbd5e1; margin-top: 8px; font-size: 14px;">{{ date }}</p>
                    </td>
                </tr>
                
                <!-- Content -->
                <tr>
                    <td class="content">
                        <!-- Featured albums -->
                        <div class="featured-section">
                            <h2 class="section-title">Featured Picks</h2>
                            <table class="featured-table" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    {% for album in featured %}
                                    <td class="featured-cell">
                                        <div class="featured-album">
                                            <!-- Match Percentage Sticker -->
                                            <div class="match-sticker" style="color: {{ get_match_color(album.percentage_match) }};">
                                                <span class="match-percentage">{{ album.percentage_match }}%</span> Match
                                            </div>
                                            <a href="{{ album.url }}"><img src="{{ album.cover_art_url }}" class="featured-cover" alt="{{ album.album_name }}" width="100%"></a>
                                            <div class="featured-details">
                                                <h3 class="featured-name">{{ album.album_name }}</h3>
                                                <p class="featured-artist">{{ album.artist }}</p>
                                                <p class="featured-blurb">{{ album.blurb }}</p>
                                                
                                                <!-- Album info section with genre -->
                                                <div class="album-info-section">
                                                    <div class="album-info-item"><strong>Genre:</strong> {{ album.genre }}</div>
                                                    <div class="album-info-item"><strong>Tracks:</strong> {{ album.track_count }}</div>
                                                    {% if album.notes %}
                                                    <div class="album-info-item"><strong>Notes:</strong> {{ album.notes|truncate(60) }}</div>
                                                    {% endif %}
                                                </div>
                                            </div>
                                            
                                            <!-- Feedback Buttons -->
                                            <div class="feedback-buttons">
                                                <a href="{{ album.feedback_base }}&album={{ album.album_id | urlencode }}&vote=up"
                                                class="feedback-btn btn-up">👍 Love It</a>
                                                
                                                <a href="{{ album.feedback_base }}&album={{ album.album_id | urlencode }}&vote=down"
                                                class="feedback-btn btn-down">👎 Not For Me</a>
                                            </div>
                                        </div>
                                    </td>
                                    {% endfor %}
                                </tr>
                            </table>
                        </div>
                        
                        <!-- Recommendations -->
                        <div class="recommendations-section">
                            <h2 class="section-title">More Recommendations For You</h2>
                            {% for row in others_rows %}
                            <table class="albums-table" cellpadding="0" cellspacing="0" border="0">
                                <tr>
                                    {% for album in row %}
                                    <td class="album-cell">
                                        <div class="album-card">
                                            <!-- Small match indicator -->
                                            <div class="small-match" style="color: {{ get_match_color(album.percentage_match) }};">
                                                {{ album.percentage_match }}%
                                            </div>
                                            <a href="{{ album.url }}"><img src="{{ album.cover_art_url }}" class="album-cover" alt="{{ album.album_name }}" width="100%"></a>
                                            <div class="album-info">
                                                <h3 class="album-name">{{ album.album_name }}</h3>
                                                <p class="album-artist">{{ album.artist }}</p>
                                                <div class="album-info-item" style="font-size: 11px; margin-top: 4px;">
                                                    {{ album.genre }} • {{ album.track_count }} tracks
                                                </div>
                                            </div>
                                            
                                            <!-- Mini Feedback Buttons -->
                                            <div style="text-align: center; margin: 8px 0 12px 0;">
                                                <a href="{{ album.feedback_base }}&album={{ album.album_id | urlencode }}&vote=up"
                                                style="text-decoration: none; font-size: 16px; margin-right: 8px; color: #334155;">👍</a>
                                                
                                                <a href="{{ album.feedback_base }}&album={{ album.album_id | urlencode }}&vote=down"
                                                style="text-decoration: none; font-size: 16px; color: #64748b;">👎</a>
                                            </div>
                                        </div>
                                    </td>
                                    {% endfor %}
                                </tr>
                            </table>
                            {% endfor %}
                        </div>
                    </td>
                </tr>
                <!-- Footer -->
                <tr>
                    <td class="footer" style="background: #0f172a; color: #ffffff; padding: 25px; text-align: center;">
                        <p style="color: #ffffff; margin: 5px 0;">Delivered by BlurryBlus • Personalized Music Discovery</p>
                        <p class="signature" style="color: #cbd5e1; font-style: italic; margin-top: 8px;">Curated by Nate</p>
                        <p>
                            <small>
                                <a href="{{ unsubscribe_url }}" style="color: #94a3b8; text-decoration: none;">Unsubscribe</a> • 
                                <a href="https://blurryblus.app" style="color: #94a3b8; text-decoration: none;">Visit Website</a>
                            </small>
                        </p>
                    </td>
                </tr>
            </table>
        </center>
    </body>
    </html>
    """).render(
        first_name=subscriber.get('first_name', 'Music Lover'),
        featured=featured_with_blurbs,
        others_rows=others_rows,
        date=run_date,
        get_match_color=get_match_color,
        unsubscribe_url=unsubscribe_url
    )

    return html

def send_email_python(**kwargs):
    """Send personalized emails to all subscribers"""
    run_date_raw = kwargs.get('ds')
    run_date = datetime.strptime(run_date_raw, "%Y-%m-%d").strftime("%A, %B %-d, %Y")
    
    ti = kwargs['ti']
    personalized_emails = ti.xcom_pull(task_ids='generate_email_content', key='personalized_emails')
    
    smtp_host = 'smtp-relay.brevo.com'
    smtp_port = 587
    login = BREVO_LOGIN 
    password = BREVO_PASSWORD 
    from_email = 'music@blurryblus.app'
    
    success_count = 0
    failure_count = 0
    
    for to_email, html_content in personalized_emails.items():
        subject = f'Your Personalized Music Recommendations - {run_date}'
        
        # Create the email message
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = "BlurryBlus <music@blurryblus.app>"
        msg['To'] = to_email
        msg.attach(MIMEText(html_content, 'html'))
        
        # Send the email
        try:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(login, password)
                server.sendmail('nathanialc17@gmail.com', to_email, msg.as_string())
            logging.info(f"Email sent successfully to {to_email}!")
            success_count += 1
        except Exception as e:
            logging.error(f"Failed to send email to {to_email}: {e}")
            failure_count += 1
    
    logging.info(f"Email sending complete. Success: {success_count}, Failures: {failure_count}")
    
    if failure_count > 0:
        raise Exception(f"Failed to send {failure_count} emails. Lookup to see whos failed.")

def skip_if_not_friday(**kwargs):
    execution_date = kwargs['execution_date']
    if execution_date.weekday() != 4:  # Friday check
        raise AirflowSkipException("Not Friday, skipping")
        
with DAG(
    'weekend_news_letter',
    default_args=default_args,
    description='Weekly music newsletter with featured albums',
    schedule=[VIEW_DATASET],  
    catchup=False,
    max_active_runs=1,
    tags=['music']
) as dag:
    
    recent_dataset_check = PythonOperator(
        task_id='check_datasets',
        python_callable=skip_if_not_friday
    )

    generate_email = PythonOperator(
        task_id='generate_email_content',
        python_callable=generate_email_content,
    )

    send_email = PythonOperator(
        task_id='send_weekly_newsletter',
        python_callable=send_email_python,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    recent_dataset_check >> generate_email >> send_email