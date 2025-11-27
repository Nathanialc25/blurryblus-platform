from flask import Flask, render_template, request, redirect, url_for
import psycopg2
from psycopg2 import IntegrityError
import os
from datetime import date, timedelta
import json
from utils.related_artist import process_user_artists
import base64

app = Flask(__name__)

# Cloud SQL connection setup
def get_connection():
    # Cloud Run environment (uses Unix socket)
    if os.environ.get('DB_USER'):
        db_user = os.environ["DB_USER"]
        db_pass = os.environ["DB_PASS"]
        db_name = os.environ["DB_NAME"] 
        cloud_sql_connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]
        
        conn = psycopg2.connect(
            user=db_user,
            password=db_pass,
            database=db_name,
            host=f"/cloudsql/{cloud_sql_connection_name}"
        )
        return conn
    else:
        # Local development
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="app_db", 
            user="postgres",
            password="Popcorn30!"
        )
        return conn

def get_available_genres():
    """Fetch distinct genres from the database using your query"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT TRIM(g) AS genre
            FROM apple_music_album_releases,
            LATERAL unnest(string_to_array(genre, ',')) AS t(g)
            WHERE TRIM(g) != 'Music'
            ORDER BY genre
        """)
        
        genres = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return genres
        
    except Exception as e:
        print(f"Error fetching genres from database: {e}")
        # Fallback to original list if database fails
        return [
            'Alternative', 'Christian', 'Country', 'Dance', 'Electronic',
            'Folk', 'Hip-Hop/Rap', 'House', 'Indie Pop', 'Indie Rock',
            'K-Pop', 'Latin', 'Metal', 'Pop', 'Rap', 'R&B/Soul', 'Rock',
            'Singer/Songwriter', 'Soundtrack', 'TV Soundtrack', 'Urbano latino'
        ]

# Get genres when the app starts - will auto-update on app restart
COMMON_GENRES = get_available_genres()

@app.route('/')
def index():
    """Displays the signup form."""
    welcome_message = "🎵 Welcome to BlurryBlus Music Recommendations! 🎵"
    return render_template('index.html', genres=COMMON_GENRES, welcome_message=welcome_message)

# Add an error route to display the error page
@app.route('/error')
def error():
    """Displays the error page."""
    error_message = request.args.get('message', 'An unexpected error occurred.')
    return render_template('error.html', error_message=error_message)

@app.route('/submit', methods=['POST'])
def submit_form():
    # Get form data
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')
    email = request.form.get('email')
    selected_genres = request.form.getlist('genres') 
    favorite_artist_input = request.form.get('favorite_artist') 
    album_length = request.form.get('album_length')       

    # Process the artists
    favorite_artists, related_artists = process_user_artists(favorite_artist_input)

    # Basic validation 
    if not first_name or not last_name or not email or not selected_genres or not album_length: 
        error_msg = "Please fill out all required fields."
        return redirect(url_for('error', message=error_msg))

    # Connect to the database using Cloud SQL connector
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Updated SQL INSERT statement to include related_artists
        insert_query = """
            INSERT INTO user_preferences (first_name, last_name, email, genres, favorite_artist, related_artists, album_length, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        genres_json = json.dumps(selected_genres)
        favorite_artists_str = ','.join(favorite_artists) 
        related_artists_str = ','.join(related_artists) 

        cursor.execute(insert_query, (first_name, last_name, email, genres_json, favorite_artists_str, related_artists_str, album_length, True))
        conn.commit()

        cursor.close()
        conn.close()

    except IntegrityError as e:
        error_msg = "This email address is already registered for this product. Please use a different email."
        return redirect(url_for('error', message=error_msg))
        
    except Exception as e:
        error_msg = f"A system error occurred. Please try again later. Error: {str(e)}"
        return redirect(url_for('error', message=error_msg))

    return redirect(url_for('success'))


@app.route('/success')
def success():
    """Success page."""
    today = date.today()
    days_until_friday = (4 - today.weekday() + 7) % 7
    if days_until_friday == 0:
        days_until_friday = 7
    next_friday = today + timedelta(days=days_until_friday)
    formatted_date = next_friday.strftime("%A, %B %-d, %Y")
    
    return render_template('success.html', formatted_date=formatted_date)


@app.route("/unsubscribe/<token>")
def unsubscribe_page(token):
    try:
        user_id = int(base64.urlsafe_b64decode(token.encode()).decode())
    except Exception:
        return render_template("error.html", error_message="Invalid unsubscribe link.")

    return render_template("unsubscribe.html", user_id=user_id)

@app.route("/unsubscribe/confirm", methods=["POST"])
def unsubscribe_confirm():
    user_id = request.form.get("user_id")
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE user_preferences SET is_active = FALSE WHERE user_id = %s", (user_id,))
        conn.commit()
        cursor.close()
        conn.close()
        # Send a message to the template
        return render_template("unsubscribe.html", user_id=user_id, banner_message="✅ You are unsubscribed!")
    except Exception as e:
        return render_template("error.html", error_message=f"Failed to unsubscribe: {str(e)}")

@app.route("/feedback")
def feedback():
    user_id = request.args.get("user")
    encoded_album_id = request.args.get("album")  # Now encoded
    vote = request.args.get("vote")

    if not user_id or not encoded_album_id or not vote:
        return render_template("error.html", error_message="Missing feedback parameters.")

    try:
        # Decode the album identifier
        album_identifier = base64.urlsafe_b64decode(encoded_album_id.encode()).decode()
        artist, album_name = album_identifier.split('|', 1)
        
        vote_value = 1 if vote == "up" else -1

        conn = get_connection()
        cursor = conn.cursor()

        # Store both the encoded ID and the artist name for learning
        cursor.execute("""
            INSERT INTO user_album_feedback (user_id, album_id, artist_name, vote)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, album_id) 
            DO UPDATE SET vote = EXCLUDED.vote, artist_name = EXCLUDED.artist_name, updated_at = NOW()
        """, (user_id, encoded_album_id, artist, vote_value))

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        return render_template("error.html", error_message=f"Database error: {str(e)}")

    return render_template("feedback_success.html")


# 10/16 adding this to allow for the ports to be dynamic. itll inject port 8080 if its getting ran by cloud run. 5000 otherwise, to work with local dev
#can remove later once things work permanently
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)