from rapidfuzz import fuzz
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_known_artists():
    '''
    fetch all known artists to anchor fuzzy matching
    '''
    hook = PostgresHook(postgres_conn_id='postgres_default')
    records = hook.get_records("SELECT DISTINCT artist FROM apple_music_album_releases")
    return [r[0] for r in records if r[0]]


def parse_list(raw):
    '''
    Parse comma-separated user inputs into clean lists
    '''
    if not raw:
        return []
    return [item.strip() for item in raw.split(',') if item.strip()]


def is_artist_match(a, b, threshold=75):
    '''
    Fuzzy-compare: multi-artist matching (fav + related)
    Return a boolean if the names are close enough.
    '''
    if not a or not b:
        return False
    return fuzz.token_set_ratio(a.lower(), b.lower()) >= threshold


def genre_similarity(album_genres, user_genres):
    '''
    Fuzzy token matching, comparing each user genre to every album genre
    Returns 0.0–1.0

    Double for loop is nuts here, ON^2? It should always be a small amount, but still...
    '''
    best = 0
    for ag in album_genres:
        for ug in user_genres:
            sim = fuzz.token_set_ratio(ag.lower(), ug.lower())
            best = max(best, sim)
    return best / 100.0  # normalize



def length_score(track_count, pref):
    """
    Range-based scoring for album length:
    - Perfect score for being within the preferred range
    - Partial credit for being close to the range
    - No points for being way off

    15 point max here
    """
    # Finding Full Matches
    if track_count is None:
        return 0
    
    if pref == 'short':
        ideal_range = range(1, 9) 
        buffer_zone = 2 
    elif pref == 'long':
        ideal_range = range(16, 50)  
        buffer_zone = 3  
    else:  # standard
        ideal_range = range(9, 16)  
        buffer_zone = 2  
    
    # Perfect match - Hopefully this is the first catch, and its within ideal range, 15 dabloons rewarded
    if track_count in ideal_range:
        return 15
  

   # Finding Partial Matches
    close_to_ideal = False
    
    if pref == 'short':
        # Can only be longer than ideal (9-10 tracks), No album has negative tracks, so we only go up
        close_to_ideal = track_count in range(ideal_range.stop, ideal_range.stop + buffer_zone)
    elif pref == 'long':
        # Can only be shorter than ideal (13-15 tracks), wont be a case of 50+ realisitically  
        close_to_ideal = track_count in range(ideal_range.start - buffer_zone, ideal_range.start)
    else:  # standard
        # Can be shorter (7-8) OR longer (16-17) than ideal
        close_to_ideal = (track_count in range(ideal_range.start - buffer_zone, ideal_range.start) or
                         track_count in range(ideal_range.stop, ideal_range.stop + buffer_zone))
    
    # Partial Match - you get 10 points
    if close_to_ideal:
        return 10
    
    # Minimal credit for being in the general ballpark
    if pref == 'short' and track_count <= 12:  # Up to 12 tracks 
        return 5
    elif pref == 'long' and track_count >= 10:  # At least 10 tracks for long preference  
        return 5
    elif pref == 'standard' and 5 <= track_count <= 20:  # Reasonable range for standard
        return 5
    
    # Way outside preferred range, gets ya a 0
    return 0


def calculate_raw_score(album, user_prefs, known_artists=None):
    """
    Returns raw integer score (0-115) for the album
    """
    score = 0

    # Clean & parse user inputs
    user_genres_raw = user_prefs.get('genres', [])
    fav_artists = parse_list(user_prefs.get('favorite_artist'))
    related_artists = parse_list(user_prefs.get('related_artists'))
    length_pref = user_prefs.get('album_length', 'standard')

    if not known_artists:
        known_artists = get_known_artists()
 
    # Handle genres - they could be string or list
    if isinstance(user_genres_raw, str):
        user_genres = parse_list(user_genres_raw)
    elif isinstance(user_genres_raw, list):
        user_genres = [str(g).strip() for g in user_genres_raw]
    else:
        user_genres = []

    # Album data
    album_artist = (album.get("artist") or "").strip()
    album_genres = [g.strip() for g in (album.get("genre") or "").split(',') if g.strip()]
    track_count = album.get("track_count", 0)

    # 1. Genre Score (0–50) 
    gsim = genre_similarity(album_genres, user_genres)
    genre_points = int(50 * gsim)  
    score += genre_points

    # 2. Favorite Artist Matches (0 or 25 each)
    for fav in fav_artists:
        if is_artist_match(fav, album_artist, threshold=80):
            score += 25

    # 3. Related Artist Matches (0–15 each) 
    for rel in related_artists:
        if is_artist_match(rel, album_artist, threshold=75):
            score += 15

    # 4. Album Length Score (0–15)
    score += length_score(track_count, length_pref)

    # 5. Synergy: artist + genre (up to +10)
    if genre_points >= 25:  # At least 50% genre match
        has_fav_match = any(is_artist_match(a, album_artist) for a in fav_artists)
        has_related_match = any(is_artist_match(r, album_artist) for r in related_artists)
        
        if has_fav_match and has_related_match:
            score += 10  # Full bonus for both
        elif has_fav_match:
            score += 6   # Good bonus for favorite
        elif has_related_match:
            score += 4   # Smaller bonus for related

    return score


def get_psychologically_adjusted_percentage(score, max_score):
    """
    Convert raw scores to psychologically better percentages:
    - More reasonable curve that doesn't over-inflate scores
    """
    raw_percentage = (score / max_score) * 100
    
    # Apply a more conservative curve
    if raw_percentage >= 40:
        # Stretch 40-100% to 60-100% (instead of 70-100%)
        final_percentage = 60 + (raw_percentage - 40) * 0.67
    else:
        # Below 40%, keep as-is
        final_percentage = raw_percentage
    
    # Ensure we don't exceed 100%
    return min(100, int(final_percentage))


def score_album(album, user_prefs, known_artists=None):
    """
    Returns psychologically adjusted percentage (0-100)
    This is the main function the DAG calls
    """
    raw_score = calculate_raw_score(album, user_prefs, known_artists)
    max_score = 115  # Fixed max for our scoring system
    return get_psychologically_adjusted_percentage(raw_score, max_score)
