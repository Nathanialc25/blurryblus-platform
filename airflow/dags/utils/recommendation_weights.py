from rapidfuzz import fuzz, process
from airflow.providers.postgres.hooks.postgres import PostgresHook


'''
9/20 definitely needs work. this recommendation system isn’t too strong. 

we now are cleaning the artist before comparing to the album artist. Need to also find a way to have multiple inputs for an artist, maybe separated by commas

Should I just match to similar artists? That would take some mapping.
'''

def get_known_artists():
    """
    Pulls all distinct artists from the full historical table.
    Returns a list of artist names.
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    records = hook.get_records("SELECT DISTINCT artist FROM apple_music_album_releases")
    return [r[0] for r in records if r[0]]

def clean_user_artist_input(user_input, known_artists=None, threshold=70):
    """
    Cleans a user's favorite artist input by matching it against known artists.
    Returns the closest known artist or None if no close match found.
    """
    if not user_input:
        return None

    user_input_clean = user_input.strip().lower()
    
    if not known_artists:
        known_artists = get_known_artists()

    # Use rapidfuzz to get best match
    match, score, _ = process.extractOne(user_input_clean, known_artists, scorer=fuzz.token_set_ratio)
    
    if score >= threshold:
        return match
    else:
        return user_input  # fallback to raw input if no close match

def is_artist_match(user_artist, album_artist, threshold=70):
    """
    Returns True if the user input artist is a close match to the album's artist.
    """
    if not user_artist or not album_artist:
        return False
    
    user_artist = user_artist.lower().strip()
    album_artist = album_artist.lower().strip()
    
    # Fuzzy similarity
    similarity = fuzz.token_set_ratio(user_artist, album_artist)
    return similarity >= threshold

def genre_score(album_genres, user_genres, max_points=30):
    """
    Score genres using Jaccard similarity.
    - Perfect match = max_points
    - Partial overlap = proportional score
    - Ignores filler tags like "Music"
    """
    if not album_genres or not user_genres:
        return 0

    # remove filler tags
    filler = {"Music"}
    album_set = {g.strip().lower() for g in album_genres if g.strip().lower() not in filler}
    user_set = {g.strip().lower() for g in user_genres if g.strip().lower() not in filler}

    if not album_set or not user_set:
        return 0
    
    overlap = album_set & user_set
    union = album_set | user_set
    
    ratio = len(overlap) / len(union)
    return round(ratio * max_points)


def calculate_album_score(album, user_prefs, known_artists=None):
    """
    Calculates a personalized score for an album based on user preferences.
    Now includes scoring for related artists.
    """
    score = 0

    # Genre Matching (Jaccard)
    album_genres = [g.strip() for g in album['genre'].split(',')] if album['genre'] else []
    user_genres = user_prefs['genres']
    genre_points = genre_score(album_genres, user_genres)
    score += genre_points

    # Favorite Artist Match (fuzzy with cleaned input)
    user_fav_artist = user_prefs.get('favorite_artist', '')
    clean_artist = clean_user_artist_input(user_fav_artist, known_artists=known_artists)
    album_artist = album.get('artist', '')
    
    # Direct artist match (highest priority)
    if is_artist_match(clean_artist, album_artist):
        score += 25 
    
    # Related artists match (medium priority)
    related_artists = user_prefs.get('related_artists', [])
    if isinstance(related_artists, str):
        related_artists = [artist.strip() for artist in related_artists.split(',') if artist.strip()]
    
    for related_artist in related_artists:
        if is_artist_match(related_artist, album_artist):
            score += 15
            break 

    # Album Length Preference
    user_length_pref = user_prefs.get('album_length', 'standard')
    track_count = album.get('track_count', 0)
    
    if user_length_pref == 'short' and track_count <= 8:
        score += 10
    elif user_length_pref == 'standard' and 6 <= track_count <= 20:
        score += 10
    elif user_length_pref == 'long' and track_count >= 15:
        score += 10

    # Synergy bonuses
    if is_artist_match(clean_artist, album_artist) and genre_points > 0:
        score += 8
    
    # Related artist + genre synergy
    related_match = any(is_artist_match(rel_artist, album_artist) for rel_artist in related_artists)
    if related_match and genre_points > 0:
        score += 5

    return score