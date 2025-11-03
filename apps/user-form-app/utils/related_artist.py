# music_utils.py
import requests
import re
import time
import psycopg2
from rapidfuzz import fuzz, process

# Add your database configuration (you might want to get these from environment variables)
DB_HOST = 'your_db_host'
DB_NAME = 'your_db_name' 
DB_USER = 'your_db_user'
DB_PASS = 'your_db_password'

def get_db_connection():
    """Create a new database connection"""
    return psycopg2.connect(
        host=DB_HOST, 
        database=DB_NAME, 
        user=DB_USER, 
        password=DB_PASS
    )

def get_apple_music_token():
    """Get Apple Music JWT token from file"""
    token_path = '/opt/airflow-docker/secrets/apple_jwt.txt'
    with open(token_path, 'r') as f:
        token_content = f.read().strip()
        token_match = re.search(r'([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)', token_content)
        return token_match.group(1) if token_match else token_content.split('\n')[0].strip()

def get_related_artists_from_apple_music(artist_name):
    """Get related artists from Apple Music API using the similar-artists endpoint"""
    try:
        headers = {'Authorization': f'Bearer {get_apple_music_token()}'}
        
        # Step 1: Get artist ID
        search_response = requests.get(
            'https://api.music.apple.com/v1/catalog/us/search',
            params={'term': artist_name, 'types': 'artists', 'limit': 1},
            headers=headers,
            timeout=10
        )
        
        if search_response.status_code != 200:
            print(f"Search API failed with status: {search_response.status_code}")
            return []
        
        artist_data = search_response.json()
        artists_list = artist_data['results']['artists'].get('data', [])
        
        if not artists_list:
            print(f"No artists found for: {artist_name}")
            return []
        
        artist_id = artists_list[0]['id']
        print(f"Found artist ID: {artist_id} for {artist_name}")
        
        # Step 2: Get similar artists using the working endpoint
        similar_artists_response = requests.get(
            f'https://api.music.apple.com/v1/catalog/us/artists/{artist_id}/view/similar-artists',
            headers=headers,
            timeout=10
        )
        
        if similar_artists_response.status_code == 200:
            similar_data = similar_artists_response.json()
            print(f"Similar artists API response received for {artist_name}")
            
            # Extract similar artists from the response
            similar_artists = extract_similar_artists_from_response(similar_data, artist_name)
            
            if similar_artists:
                print(f"Found {len(similar_artists)} similar artists for {artist_name}")
                return similar_artists[:10]  # Return max 10 artists
        
        # If similar artists endpoint fails, fall back to genre-based approach
        print(f"Similar artists endpoint failed, falling back to genre-based approach for {artist_name}")
        return get_related_artists_by_genre(artist_id, artist_name, headers)
    
    except Exception as e:
        print(f"Apple Music API failed for {artist_name}: {e}")
        return []

def extract_similar_artists_from_response(similar_data, original_artist_name):
    """Extract similar artists from the API response"""
    similar_artists = []
    
    # Parse the response structure from endpoint 3
    if 'data' in similar_data and isinstance(similar_data['data'], list):
        for artist in similar_data['data']:
            if 'attributes' in artist and 'name' in artist['attributes']:
                artist_name = artist['attributes']['name']
                # Exclude the original artist
                if artist_name.lower() != original_artist_name.lower():
                    similar_artists.append(artist_name)
    
    # Alternative: Check results structure if data is empty
    elif 'results' in similar_data:
        results = similar_data['results']
        # Look for similar artists in results
        for key in results:
            if key in ['similar-artists', 'artists'] and 'data' in results[key]:
                for artist in results[key]['data']:
                    if 'attributes' in artist and 'name' in artist['attributes']:
                        artist_name = artist['attributes']['name']
                        if artist_name.lower() != original_artist_name.lower():
                            similar_artists.append(artist_name)
    
    return similar_artists

def get_related_artists_by_genre(artist_id, artist_name, headers):
    """Fallback: Get related artists by genre when similar artists endpoint fails"""
    try:
        # Get artist genre
        artist_response = requests.get(
            f'https://api.music.apple.com/v1/catalog/us/artists/{artist_id}',
            headers=headers,
            timeout=10
        )
        
        if artist_response.status_code != 200:
            return []
        
        artist_info = artist_response.json()
        genres = artist_info['data'][0]['attributes'].get('genreNames', [])
        
        if not genres:
            return []
        
        # Search for artists in the same genre
        genre_response = requests.get(
            'https://api.music.apple.com/v1/catalog/us/search',
            params={'term': genres[0], 'types': 'artists', 'limit': 15},
            headers=headers,
            timeout=10
        )
        
        if genre_response.status_code != 200:
            return []
        
        genre_artists = genre_response.json()['results']['artists']['data']
        
        # Return artist names (excluding the original), max 10
        return [
            artist['attributes']['name'] 
            for artist in genre_artists 
            if artist['attributes']['name'].lower() != artist_name.lower()
        ][:10]
    
    except Exception as e:
        print(f"Genre-based fallback failed for {artist_name}: {e}")
        return []

def get_related_artists_from_database(artist_name):
    """Fallback: find similar artists from your existing album data"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Find this artist's main genre from historical data
            genre_query = """
                SELECT genre, COUNT(*) as count 
                FROM apple_music_album_releases 
                WHERE artist ILIKE %s 
                GROUP BY genre 
                ORDER BY count DESC 
                LIMIT 1
            """
            cursor.execute(genre_query, (f'%{artist_name}%',))
            genre_result = cursor.fetchone()
            
            if not genre_result or not genre_result[0]:
                return []
            
            main_genre = genre_result[0]
            
            # Find other artists in the same genre
            similar_artists_query = """
                SELECT DISTINCT artist 
                FROM apple_music_album_releases 
                WHERE genre ILIKE %s 
                AND artist NOT ILIKE %s
                ORDER BY RANDOM()
                LIMIT 12
            """
            cursor.execute(similar_artists_query, (f'%{main_genre}%', f'%{artist_name}%'))
            results = cursor.fetchall()
            
            return [r[0] for r in results if r[0]]
    
    except Exception as e:
        print(f"Database fallback failed for {artist_name}: {e}")
        return []
    finally:
        if conn:
            conn.close()

def fuzzy_deduplicate_artists(artists, threshold=80):
    """Remove duplicates using fuzzy matching"""
    unique_artists = []
    for artist in artists:
        # Check if this artist is similar to any already added
        if not any(fuzz.ratio(artist.lower(), existing.lower()) > threshold 
                  for existing in unique_artists):
            unique_artists.append(artist)
    return unique_artists

def get_related_artists(artist_name, limit=10):
    """Get related artists with fallback strategy"""
    # Try Apple Music API first
    apple_music_artists = get_related_artists_from_apple_music(artist_name)
    
    if apple_music_artists:
        return apple_music_artists[:limit]
    
    # Fallback to database lookup
    db_artists = get_related_artists_from_database(artist_name)
    return db_artists[:limit]

def calculate_fair_distribution(num_artists, total_slots):
    """Calculate how many related artists to get from each input artist"""
    if num_artists == 0:
        return []
    
    # Base calculation: divide total slots as evenly as possible
    base_count = total_slots // num_artists
    remainder = total_slots % num_artists
    
    distribution = []
    
    for i in range(num_artists):
        # Give extra slots to the first few artists if there's a remainder
        count = base_count + (1 if i < remainder else 0)
        distribution.append(count)
    
    return distribution

def process_user_artists(artist_input, max_related=20):
    """Process multiple artists with fair distribution of related artists"""
    favorite_artists = [artist.strip() for artist in artist_input.split(',') if artist.strip()]
    
    if not favorite_artists:
        return [], []
    
    # Calculate how many related artists to get per input artist
    num_favorites = len(favorite_artists)
    artists_per_source = calculate_fair_distribution(num_favorites, max_related)
    
    print(f"Getting {max_related} total related artists from {num_favorites} input artists")
    print(f"Distribution: {artists_per_source}")
    
    all_related_artists = []
    
    for i, artist in enumerate(favorite_artists):
        try:
            # Get the allocated number of related artists for this artist
            target_count = artists_per_source[i]
            related = get_related_artists(artist)
            
            # Take only the allocated number (or all available if fewer)
            taken_artists = related[:target_count]
            all_related_artists.extend(taken_artists)
            
            print(f"Got {len(taken_artists)} related artists for '{artist}' (target: {target_count})")
            
            # Rate limiting between API calls
            if i < len(favorite_artists) - 1:
                time.sleep(0.2)
                
        except Exception as e:
            print(f"Failed to process artist '{artist}': {e}")
            continue
    
    # Remove duplicates with fuzzy matching
    unique_related = fuzzy_deduplicate_artists(all_related_artists)
    
    # Remove any favorites that accidentally ended up in related artists
    final_related = [
        artist for artist in unique_related 
        if not any(fuzz.ratio(artist.lower(), fav.lower()) > 85 
                  for fav in favorite_artists)
    ]
    
    print(f"Final result: {len(favorite_artists)} favorite artists, {len(final_related)} related artists")
    
    return favorite_artists, final_related[:max_related]