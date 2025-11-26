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
    similarity = fuzz.token_set_ratio(a.lower(), b.lower())
    return similarity >= threshold


def genre_similarity(album_genres, user_genres):
    '''
    Fuzzy token matching, comparing each user genre to every album genre
    Returns 0.0–1.0
    '''
    best = 0
    best_match = ("", "")
    
    for ag in album_genres:
        for ug in user_genres:
            sim = fuzz.token_set_ratio(ag.lower(), ug.lower())
            if sim > best:
                best = sim
                best_match = (ag, ug)
    
    # DEBUG: Show best genre match
    print(f" Best genre match: '{best_match[0]}' vs '{best_match[1]}' = {best}%")
    return best / 100.0


def length_score(track_count, pref):
    """
    Range-based scoring for album length
    """
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
    
    # Perfect match
    if track_count in ideal_range:
        print(f"Length: {track_count} tracks -> PERFECT match for {pref} (+15)")
        return 15
  
    # Partial Matches
    close_to_ideal = False
    
    if pref == 'short':
        close_to_ideal = track_count in range(ideal_range.stop, ideal_range.stop + buffer_zone)
    elif pref == 'long':
        close_to_ideal = track_count in range(ideal_range.start - buffer_zone, ideal_range.start)
    else:  # standard
        close_to_ideal = (track_count in range(ideal_range.start - buffer_zone, ideal_range.start) or
                         track_count in range(ideal_range.stop, ideal_range.stop + buffer_zone))
    
    if close_to_ideal:
        print(f"Length: {track_count} tracks -> CLOSE match for {pref} (+10)")
        return 10
    
    # Minimal credit
    if pref == 'short' and track_count <= 12:
        print(f" Length: {track_count} tracks -> BALLPARK match for {pref} (+5)")
        return 5
    elif pref == 'long' and track_count >= 10:
        print(f" Length: {track_count} tracks -> BALLPARK match for {pref} (+5)")
        return 5
    elif pref == 'standard' and 5 <= track_count <= 20:
        print(f" Length: {track_count} tracks -> BALLPARK match for {pref} (+5)")
        return 5
    
    print(f"Length: {track_count} tracks -> NO match for {pref} (+0)")
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

    # 🐛 DEBUG: Show input data
    print(f"DEBUG: {album['artist']} - {album['album_name']}")
    print(f"    Album genres: {album_genres}")
    print(f"    User genres: {user_genres}")
    print(f"    Fav artists: {fav_artists}")
    print(f"    Related artists: {related_artists}")

    # 1. Genre Score (0–50) 
    gsim = genre_similarity(album_genres, user_genres)
    genre_points = int(50 * gsim)
    score += genre_points
    print(f"    🎵 Genre score: {genre_points}/50")

    # 2. Favorite Artist Matches (0 or 25 each)
    fav_matches = []
    for fav in fav_artists:
        if is_artist_match(fav, album_artist, threshold=80):
            score += 25
            fav_matches.append(fav)
            print(f"Favorite artist match: '{fav}' -> +25")

    # 3. Related Artist Matches (0–15 each) 
    related_matches = []
    for rel in related_artists:
        if is_artist_match(rel, album_artist, threshold=75):
            score += 15
            related_matches.append(rel)
            print(f"Related artist match: '{rel}' -> +15")

    # 4. Album Length Score (0–15)
    length_points = length_score(track_count, length_pref)
    score += length_points

    # 5. Synergy: artist + genre (up to +10)
    synergy_bonus = 0
    if genre_points >= 25:  # At least 50% genre match
        has_fav_match = len(fav_matches) > 0
        has_related_match = len(related_matches) > 0
        
        if has_fav_match and has_related_match:
            synergy_bonus = 10
            print(f"Synergy: favorite + related artist -> +10")
        elif has_fav_match:
            synergy_bonus = 6
            print(f" Synergy: favorite artist -> +6")
        elif has_related_match:
            synergy_bonus = 4
            print(f"Synergy: related artist -> +4")
        
        score += synergy_bonus

    # 🐛 DEBUG: Show final breakdown
    print(f" FINAL BREAKDOWN:")
    print(f" Genre: {genre_points}")
    print(f" Artists: {len(fav_matches)*25 + len(related_matches)*15}")
    print(f" Length: {length_points}") 
    print(f" Synergy: {synergy_bonus}")
    print(f" TOTAL RAW: {score}/115")
    
    raw_percent = (score / 115) * 100
    final_percent = get_psychologically_adjusted_percentage(score, 115)
    print(f"       RAW %: {raw_percent:.1f}% -> FINAL %: {final_percent}%")
    print("---")

    return score


def get_psychologically_adjusted_percentage(score, max_score):
    """
    Convert raw scores to psychologically better percentages
    """
    raw_percentage = (score / max_score) * 100
    
    # Apply curve
    if raw_percentage >= 40:
        final_percentage = 60 + (raw_percentage - 40) * 0.67
    else:
        final_percentage = raw_percentage
    
    return min(100, int(final_percentage))


def get_intelligent_feedback_weights(user_id, user_stated_genres):
    """
    Smart feedback analysis
    """
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
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
    
    # Artist-level learning
    for artist, data in artist_analysis.items():
        total_votes = len(data['votes'])
        net_score = sum(data['votes'])
        
        if total_votes >= 2 and net_score < -1:
            artist_weights[artist] = -1.0
            print(f"Suppressing artist: {artist} ({net_score} across {total_votes} votes)")
        elif total_votes >= 2 and net_score > 1:
            artist_weights[artist] = 0.3
            print(f"Boosting artist: {artist} ({net_score} across {total_votes} votes)")
    
    # Genre-level learning
    for genre, data in genre_analysis.items():
        unique_artists = len(data['artists'])
        total_votes = len(data['votes'])
        net_score = sum(data['votes'])
        
        if unique_artists >= 2 and total_votes >= 3:
            approval_ratio = (net_score + total_votes) / (2 * total_votes)
            
            if approval_ratio < 0.3:
                genre_weights[genre] = 0.3
                print(f"Suppressing genre: {genre} ({approval_ratio:.2f} approval)")
            elif approval_ratio > 0.7:
                genre_weights[genre] = 1.3
                print(f"Boosting genre: {genre} ({approval_ratio:.2f} approval)")
    
    # Discovery learning
    discovery_weights = analyze_genre_discovery(genre_analysis, user_stated_genres)
    genre_weights.update(discovery_weights)
    
    return artist_weights, genre_weights


def analyze_genre_discovery(genre_analysis, user_stated_genres):
    """
    Detect when users like genres they didn't originally state
    """
    discovery_weights = {}
    
    for genre, data in genre_analysis.items():
        if genre not in user_stated_genres:
            unique_artists = len(data['artists'])
            total_votes = len(data['votes'])
            net_score = sum(data['votes'])
            
            if unique_artists >= 2 and total_votes >= 3 and net_score > 1:
                approval_ratio = (net_score + total_votes) / (2 * total_votes)
                if approval_ratio > 0.6:
                    discovery_weights[genre] = 1.2
                    print(f"Discovery boost: {genre} (not in stated preferences)")
    
    return discovery_weights


def calculate_raw_score_with_precomputed_weights(album, user_prefs, known_artists, artist_weights, genre_weights):
    """
    Use pre-fetched weights to avoid repeated database queries
    """
    base_score = calculate_raw_score(album, user_prefs, known_artists)
    
    # Apply artist feedback
    album_artist = (album.get("artist") or "").strip()
    if album_artist in artist_weights:
        weight = artist_weights[album_artist]
        if weight == -1.0:
            return 0
        else:
            base_score = base_score * (1 + weight)
    
    # Apply genre feedback
    album_genres = [g.strip() for g in (album.get("genre") or "").split(',') if g.strip()]
    for genre in album_genres:
        if genre in genre_weights:
            genre_weight = genre_weights[genre]
            if genre_weight < 0.5:
                base_score = base_score * genre_weight
            else:
                base_score = base_score * genre_weight
    
    return min(115, int(base_score))


