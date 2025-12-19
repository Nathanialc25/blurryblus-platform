from datetime import datetime, timedelta
import sys
import os

import requests
import logging
import hashlib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import Dataset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

JWT_DATASET = Dataset("dataset://apple/jwt")
VIEW_DATASET = Dataset("view://apple_music/v_weekly_new_releases")

STORE_FRONT = 'US'
TABLE_NAME = "apple_music_album_releases"
VIEW_NAME = "v_weekly_new_releases"
BASE_DIR = os.path.dirname(__file__) 
SECRETS_DIR = os.path.join(BASE_DIR, "secrets")
JWT_PATH = os.path.join(SECRETS_DIR, "apple_jwt.txt")
SCHEMA = 'public'

PLAYLISTS = [
    {
        'id': "pl.2b0e6e332fdf4b7a91164da3162127b5", # New Music Daily
        'name': 'NMD'
    },
    {
        'id': "pl.f4d106fed2bd41149aaacabb233eb5eb", # Today's Hits
        'name': 'Todays hits'
    },
    {
        'id': "pl.1fa57a04cd794a8aa482a3492f26fbcd", # New hip hop
        'name': 'New hiphop'
    },
    {
        'id': "pl.f19f6b5be8474fe789e36a6242f6113e", # New Fire
        'name': 'New Fire'
    },
    {
        'id': "pl.baa060f67ea94488a6e0c7e90c8afdb0", # New in R&B
        'name': "New in R&B"
    },
    {
        'id': "pl.bcb2f44b6e194cfa8950a796b4e65cd1", # Alpha Music
        'name': "Alpha Music"
    },
    {
        'id':"pl.2b426ec1994e4120910214dab840c927", # Alternative
        'name': "Heaps Indie"
    },
    {
        'id':"pl.3652c8971d244ec688479db7f7599f87", # Heavy hitters Apple Music Dance
        'name':"Apple Music Dance"
    },
    {
        'id':"pl.dc349df19c6f410d874c197db63ecfed", # afrobeats hits
        'name':"Afrobeats Hits"
    },
    {
        'id':"pl.07405f59596b402385451fa14695eec4", # Jazz Currents
        'name':"Jazz Currents"
    },
]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    complex_id VARCHAR(100) PRIMARY KEY,
    album_name TEXT NOT NULL,
    artist TEXT NOT NULL,
    release_date DATE NOT NULL,
    track_count INTEGER,
    genre TEXT ,
    url TEXT UNIQUE NOT NULL,
    editorial_notes TEXT,
    cover_art_url TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_album UNIQUE (complex_id)
);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    complex_id, 
    album_name,
    artist, 
    release_date, 
    track_count, 
    genre, 
    url, 
    editorial_notes, 
    cover_art_url
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (url) DO NOTHING;
"""

VIEW_SQL = f"""
DROP VIEW IF EXISTS v_weekly_new_releases;

CREATE VIEW {VIEW_NAME} AS
    SELECT 
        complex_id, 
        album_name,
        artist, 
        release_date, 
        track_count, 
        array_to_string(array_remove(string_to_array(genre, ','), ' Music'), ',') as genre,
        url, 
        editorial_notes, 
        cover_art_url
    FROM {SCHEMA}.{TABLE_NAME}
    WHERE 
        DATE(release_date) BETWEEN (CURRENT_DATE - INTERVAL '6 days') AND CURRENT_DATE
"""

def generate_album_id(album_data: dict) -> str:
    '''
    Creates a hash encoded unique id for each album release
    Will be useful later when trying to ensure no dupes in our table.
    '''
    unique_string = f"{album_data.get('release_date','')} | {album_data.get('artist','')} | {album_data.get('url','')}"
    return hashlib.sha256(unique_string.encode()).hexdigest() #hashing is good here, but maybe theres somethign simpler? concat not it, but something else

#first check, then a sub field check seems weird, maybe I should just go to the root?
def get_album_artwork(artwork_data):
    '''
    Returns a 600x600 artwork URL from an Apple Music artwork payload.
    '''
    if not artwork_data or not artwork_data.get('url'):
        return None
    return artwork_data['url'].replace('{w}x{h}', '600x600')

def get_jwt_token():
    '''
    Grabs our JWT token to access the API
    SHouldve been created by the apple_music_token_generation DAG.
    '''
    with open(JWT_PATH, 'r') as file:
        return file.readline().strip()

def get_headers():
    '''
    Grabbing HTTP headers that are necessary for the API request
    '''
    return {"Authorization": f"Bearer {get_jwt_token()}"}

def fetch_playlist_data(**kwargs):
    '''
    Grab songs from the Popular playlist, creates a list of dicts, that has album information. Also filters out -Singles.
    Most importantly grabbing the album names to search them in downstream.
    '''
    all_songs = []
    
    for playlist in PLAYLISTS:
        logging.info(f"Fetching data from playlist: {playlist['name']} ({playlist['id']})")
        
        url = f"https://api.music.apple.com/v1/catalog/{STORE_FRONT}/playlists/{playlist['id']}"
        response = requests.get(url, headers=get_headers())
        
        if response.status_code != 200:
            logging.warning(f"Failed to fetch playlist {playlist['name']}: {response.status_code}. Check the id...")
            continue

        playlist_data = response.json()
        songs = playlist_data.get('data', [])[0].get('relationships', {}).get('tracks', {}).get('data', [])

        # List comprehension, but each element in the list is a dict
        trending_songs = [
            {
                "song_name": song.get('attributes', {}).get('name', 'Unknown Song'),
                "album_name": song.get('attributes', {}).get('albumName', 'Unknown Album'),
                "artist": song.get('attributes', {}).get('artistName', 'Unknown Artist'),
                "playlist_source": playlist['name']  # Track which playlist it came from
            }
            for song in songs
            if '- Single' not in song.get('attributes', {}).get('albumName', '')
        ]
        #extend over append, extend throws every element into the list as an element, as opposed to just adding one element to the end of the list
        all_songs.extend(trending_songs)
        logging.info(f"Found {len(trending_songs)} songs from {playlist['name']}")
    
    kwargs['ti'].xcom_push(key='reduced_songs', value=all_songs)
    logging.info(f"Total songs collected: {len(all_songs)}")
    return all_songs

def fetch_album_details(**kwargs):
    '''
    Searching Apple Music for the album, and extract more information to add to album details.
    Then filtering the albums down to only ones from the last 7 days
    '''
    ti = kwargs['ti']
    reduced_songs = ti.xcom_pull(task_ids='fetch_playlist_data', key='reduced_songs')
    base_url = f"https://api.music.apple.com/v1/catalog/{STORE_FRONT}/search"
    
    album_details = []
    for song in reduced_songs:
        #term is a crazy thing, it is more or less a straight up search, its just a search, grab an album, and then limit by 1. and hoping its the correct one.
        params = {"term": f"{song['album_name']} {song['artist']}", "types": "albums", "limit": 1}
        response = requests.get(base_url, headers=get_headers(), params=params)
        if response.status_code == 200:
            data = response.json()
            if albums := data.get('results', {}).get('albums', {}).get('data', []):
                album = albums[0]['attributes']
                album_details.append({
                    'album_name': album.get('name'),
                    'artist': album.get('artistName'),
                    'release_date': album.get('releaseDate'),
                    'track_count': album.get('trackCount'),
                    'genre': ', '.join(album.get('genreNames', [])),
                    'url': album.get('url'),
                    'editorial_notes': album.get('editorialNotes', {}).get('short', ''),
                    'cover_art': get_album_artwork(album.get('artwork'))
                })
        else:
            logging.warning(f"Failed to fetch Album data. {song}: {response.status_code}")

    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    #Must filter the albums down to what has been dropped in the last 7 days
    recent_albums = [album for album in album_details if album['release_date'] and album['release_date'] >= seven_days_ago and '- Single' not in album['album_name'] ]
    
    kwargs['ti'].xcom_push(key='recent_albums', value=recent_albums)
    return recent_albums

def store_album_data(**kwargs):
    '''
    Pushing all the data for albums into Cloud SQL/Postgres db
    '''
    ti = kwargs['ti']
    recent_albums = ti.xcom_pull(task_ids='fetch_album_details', key='recent_albums')
    if not recent_albums:
        logging.info("No recent albums found to store")
        return

    postgres_hook = PostgresHook(postgres_conn_id='postgres_default') # 127 host?
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        for album in recent_albums:
            #catch for data without a release date. unsure if this is needed at this point, may been human error
            release_date = None
            if album.get('release_date'):
                try:
                    release_date = datetime.strptime(album['release_date'], '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    pass

            #double checking on these track counts
            track_count = int(album.get('track_count', 0)) if str(album.get('track_count', '0')).isdigit() else 0

            #hash for the album, to avoid dup entries
            album_id = generate_album_id(album)
            
            cursor.execute(UPSERT_SQL, (
                album_id,
                str(album.get('album_name', '')),
                str(album.get('artist', '')),
                release_date,
                track_count,
                str(album.get('genre', '')),
                str(album.get('url', '')),
                str(album.get('editorial_notes', '')),
                album.get('cover_art')
            ))
        
        conn.commit()
        logging.info(f"Processed {len(recent_albums)} albums (duplicates skipped)")
    
    except Exception as e:
        conn.rollback()
        logging.error(f"Error storing album data: {str(e)}")
        raise

    finally:
        cursor.close()
        conn.close()

with DAG(
    'apple_music_album_extraction',
    default_args=default_args,
    description='Fetches recent trending audio from multiple playlists and pulls album info to Cloud SQL',
    schedule=[JWT_DATASET],
    catchup=False,
    tags=['music', 'etl'],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql=CREATE_TABLE_SQL
    )

    fetch_playlist = PythonOperator(
        task_id='fetch_playlist_data',
        python_callable=fetch_playlist_data,
    )
    
    fetch_albums = PythonOperator(
        task_id='fetch_album_details',
        python_callable=fetch_album_details,
    )

    store_data = PythonOperator(
        task_id='store_album_data',
        python_callable=store_album_data,
    )
    
    create_view = SQLExecuteQueryOperator(
        task_id='create_view',
        conn_id='postgres_default',
        sql=VIEW_SQL,
        outlets=[VIEW_DATASET]
    )

    create_table >> fetch_playlist >> fetch_albums >> store_data >> create_view