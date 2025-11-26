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


def get_intelligent_feedback_weights(user_id, user_stated_genres):
    """
    Smart feedback analysis that distinguishes between:
    - Artist-specific dislike (multiple albums from same artist)
    - Genre-wide dislike (multiple artists from same genre)
    - Genre discovery (liking genres outside stated preferences)
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get comprehensive voting history with genre context
    feedback_query = """
        SELECT 
            uf.artist_name,
            am.genre as album_genres,
            uf.vote,
            COUNT(*) OVER (PARTITION BY uf.artist_name) as votes_for_artist
        FROM user_album_feedback uf
        LEFT JOIN apple_music_album_releases am ON uf.artist_name = am.artist
        WHERE uf.user_id = %s AND uf.artist_name IS NOT NULL
        ORDER BY uf.created_at DESC
    """
    
    records = hook.get_records(feedback_query, parameters=(user_id,))
    
    artist_analysis = {}
    genre_analysis = {}
    
    for artist, album_genres, vote, votes_for_artist in records:
        # Clean genres
        genres_list = [g.strip() for g in (album_genres or "").split(',') if g.strip()]
        
        # Analyze artist patterns
        if artist not in artist_analysis:
            artist_analysis[artist] = {'votes': [], 'genres': set(), 'total_votes': 0}
        artist_analysis[artist]['votes'].append(vote)
        artist_analysis[artist]['genres'].update(genres_list)
        artist_analysis[artist]['total_votes'] = votes_for_artist
        
        # Analyze genre patterns
        for genre in genres_list:
            if genre not in genre_analysis:
                genre_analysis[genre] = {'artists': set(), 'votes': []}
            genre_analysis[genre]['artists'].add(artist)
            genre_analysis[genre]['votes'].append(vote)
    
    # Calculate intelligent weights
    artist_weights = {}
    genre_weights = {}
    
    # 1. ARTIST-LEVEL LEARNING: Single artist patterns
    for artist, data in artist_analysis.items():
        total_votes = len(data['votes'])
        net_score = sum(data['votes'])
        
        # Strong artist dislike: multiple downvotes for same artist
        if total_votes >= 2 and net_score < -1:
            artist_weights[artist] = -1.0  # Complete suppression
            print(f"Suppressing artist: {artist} ({net_score} across {total_votes} votes)")
        
        # Strong artist like: multiple upvotes for same artist  
        elif total_votes >= 2 and net_score > 1:
            artist_weights[artist] = 0.3  # Boost artist
            print(f"Boosting artist: {artist} ({net_score} across {total_votes} votes)")
    
    # 2. GENRE-LEVEL LEARNING: Multi-artist genre patterns
    for genre, data in genre_analysis.items():
        unique_artists = len(data['artists'])
        total_votes = len(data['votes'])
        net_score = sum(data['votes'])
        
        # Only analyze genres with sufficient data
        if unique_artists >= 2 and total_votes >= 3:
            approval_ratio = (net_score + total_votes) / (2 * total_votes)  # Convert to 0-1 scale
            
            # Genre-wide dislike: multiple artists in same genre downvoted
            if approval_ratio < 0.3:
                genre_weights[genre] = 0.3  # Heavy genre suppression
                print(f"Suppressing genre: {genre} ({approval_ratio:.2f} approval, {unique_artists} artists)")
            
            # Genre-wide like: multiple artists in same genre upvoted
            elif approval_ratio > 0.7:
                genre_weights[genre] = 1.3  # Genre boost
                print(f"Boosting genre: {genre} ({approval_ratio:.2f} approval, {unique_artists} artists)")
    
    # 3. DISCOVERY LEARNING: Preferences outside stated genres
    discovery_weights = analyze_genre_discovery(genre_analysis, user_stated_genres)
    genre_weights.update(discovery_weights)
    
    return artist_weights, genre_weights

def analyze_genre_discovery(genre_analysis, user_stated_genres):
    """
    Detect when users like genres they didn't originally state
    """
    discovery_weights = {}
    
    for genre, data in genre_analysis.items():
        # Only consider genres NOT in user's stated preferences
        if genre not in user_stated_genres:
            unique_artists = len(data['artists'])
            total_votes = len(data['votes'])
            net_score = sum(data['votes'])
            
            # Discovery pattern: multiple upvotes for non-preferred genre
            if unique_artists >= 2 and total_votes >= 3 and net_score > 1:
                approval_ratio = (net_score + total_votes) / (2 * total_votes)
                if approval_ratio > 0.6:
                    discovery_weights[genre] = 1.2  # Moderate discovery boost
                    print(f"Discovery boost: {genre} (not in stated preferences)")
    
    return discovery_weights

def calculate_raw_score_with_feedback(album, user_prefs, known_artists=None):
    """
    Enhanced scoring that incorporates intelligent feedback learning
    """
    base_score = calculate_raw_score(album, user_prefs, known_artists)
    
    # Get user feedback weights if user_id is available
    user_id = user_prefs.get('user_id')
    user_stated_genres = user_prefs.get('genres', [])
    
    if user_id and user_stated_genres:
        artist_weights, genre_weights = get_intelligent_feedback_weights(user_id, user_stated_genres)
        
        # Apply artist feedback (highest priority - complete suppression)
        album_artist = (album.get("artist") or "").strip()
        if album_artist in artist_weights:
            weight = artist_weights[album_artist]
            if weight == -1.0:  # Artist completely suppressed
                return 0  # Zero score for suppressed artists
            else:
                base_score = base_score * (1 + weight)  # Apply artist boost
        
        # Apply genre feedback (moderate influence)
        album_genres = [g.strip() for g in (album.get("genre") or "").split(',') if g.strip()]
        for genre in album_genres:
            if genre in genre_weights:
                genre_weight = genre_weights[genre]
                if genre_weight < 0.5:  # Genre suppression
                    base_score = base_score * genre_weight
                else:  # Genre boost
                    base_score = base_score * genre_weight
    
    return min(115, int(base_score))  # Cap at max score

def score_album_with_feedback(album, user_prefs, known_artists=None):
    """
    Main scoring function that incorporates intelligent feedback learning
    Use this in your DAG instead of score_album for learning capabilities
    """
    raw_score = calculate_raw_score_with_feedback(album, user_prefs, known_artists)
    max_score = 115
    return get_psychologically_adjusted_percentage(raw_score, max_score)
