# Data loading code (step 3 of the Phase 4 instructions)

# Import necessary libraries
import pymongo
import pandas as pd
from pathlib import Path

# IMPORTANT: UPDATE THESE BEFORE RUNNING
CWL = 'xxx'
SNUM = 'xxx'

# Paths
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "processed_data"

# Helper functions for CSV processing (same as in generate_oracle_sql.py)
def clean_text(value):
    if value is None:
        return ""
    return str(value).strip()
    
def normalize_text(value):
    return clean_text(value).lower()

def is_missing(value):
    return clean_text(value) == ""

def make_song_key(title, artist):
    return (normalize_text(title), normalize_text(artist))

def should_replace_song(old_row, new_row):
    old_date = old_row["release_date"]
    new_date = new_row["release_date"]

    if is_missing(old_date):
        return True
    if is_missing(new_date):
        return False
    if new_date < old_date:
        return True
    if new_date > old_date:
        return False

    old_track = clean_text(old_row["track_name"])
    new_track = clean_text(new_row["track_name"])
    if new_track < old_track:
        return True
    if new_track > old_track:
        return False

    old_track_id = clean_text(old_row["spotify_track_id"])
    new_track_id = clean_text(new_row["spotify_track_id"])
    if old_track_id == "":
        return new_track_id != ""
    if new_track_id == "":
        return False
    return new_track_id < old_track_id

# Check if CWL and SNUM have been updated from the placeholder values (from the 
# "Connecting to MongoDB via Python" Canvas page)
if CWL.strip() == "" or CWL == 'xxx' or SNUM.strip() == "" or SNUM == 'xxx':
    print("You need up to update the value of the CWL and/or SNUM variables before proceeding.")
else:
    # Connect to MongoDB
    connection_string = f"mongodb://{CWL}:a{SNUM}@localhost:27017/{CWL}"
    client = pymongo.MongoClient(connection_string)
    db = client[CWL]

    # Load csv data into pandas DataFrames
    track = pd.read_csv(DATA_DIR / "clean_spotify_track.csv")
    top200 = pd.read_csv(DATA_DIR / "clean_spotify_top_200.csv")
    tiktok = pd.read_csv(DATA_DIR / "clean_tiktok.csv")

    # Unique songs from top200 + tiktok, deduplicated by (canonical_title, main_artist) (Same as Phase 3)
    all_rows = []
 
    for _, row in top200.iterrows():
        all_rows.append({
            "canonical_title": clean_text(row["canonical_title"]),
            "main_artist": clean_text(row["main_artist"]),
            "track_name": clean_text(row["track_name"]),
            "artist": clean_text(row["artist_name"]),
            "release_date": clean_text(row["release_date"]),
            "spotify_track_id": clean_text(row.get("track_id", "")),
        })
    
    for _, row in tiktok.iterrows():
        all_rows.append({
            "canonical_title": clean_text(row["canonical_title"]),
            "main_artist": clean_text(row["main_artist"]),
            "track_name": clean_text(row["track_name"]),
            "artist": clean_text(row["artist_name"]),
            "release_date": clean_text(row["release_date"]),
            "spotify_track_id": clean_text(row.get("track_id", "")),
        })
    
    # Deduplicate using the same should_replace_song logic as Phase 3
    unique_songs = {}
    for row in all_rows:
        key = make_song_key(row["canonical_title"], row["main_artist"])
        if key not in unique_songs or should_replace_song(unique_songs[key], row):
            unique_songs[key] = row
    
    print(f"Unique songs: {len(unique_songs)}")

    # Genre lookup from spotify_track (Same as Phase 3)
    genre_lookup = {}
    for _, row in track.iterrows():
        key = make_song_key(row["canonical_title"], row["main_artist"])
        genre_lookup[key] = normalize_text(row["track_genre"])

    # Build chart_history from top200
    chart_history = {}
    for _, row in top200.iterrows():
        key = make_song_key(row["canonical_title"], row["main_artist"])
        week = clean_text(row["week"])
        rank = int(row["rank"])
    
        if key not in chart_history:
            chart_history[key] = {}
    
        # Keep the best (lowest) rank per week, same as Phase 3
        if week not in chart_history[key] or rank < chart_history[key][week]:
            chart_history[key][week] = rank
    
    # Build tiktok lookup
    tiktok_lookup = {}
    for _, row in tiktok.iterrows():
        key = make_song_key(row["canonical_title"], row["main_artist"])
        popularity = int(row["popularity"])
    
        if key not in tiktok_lookup or popularity > tiktok_lookup[key]:
            tiktok_lookup[key] = popularity
    
    # Assemble MongoDB documents
    documents = []
    for song_id, (key, song) in enumerate(unique_songs.items(), start=1):
        doc = {
            "song_id": song_id,
            "track_name": song["track_name"],
            "artist": song["artist"],
            "genre": genre_lookup.get(key, None),
            "canonical_title": song["canonical_title"],
            "release_date": song["release_date"],
            "spotify_track_id": song["spotify_track_id"] if not is_missing(song["spotify_track_id"]) else None,
        }
    
        # Only add tiktok subdocument the song has tiktok data
        if key in tiktok_lookup:
            doc["tiktok"] = {"popularity": tiktok_lookup[key]}
    
        # Only add chart_history if the song has chart data
        if key in chart_history:
            doc["chart_history"] = [
                {"week": week, "rank": rank}
                for week, rank in sorted(chart_history[key].items())
            ]
    
        documents.append(doc)
    
    # Insert into MongoDB
    db["song"].drop()
    result = db["song"].insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} documents into 'song'")

    # Verify
    print(f"\nTotal documents: {db['song'].count_documents({})}")
    print(f"With chart_history: {db['song'].count_documents({'chart_history': {'$exists': True}})}")
    print(f"With tiktok: {db['song'].count_documents({'tiktok': {'$exists': True}})}")
    print(f"With genre: {db['song'].count_documents({'genre': {'$ne': None}})}")

    print("\n--- Sample document (has both chart_history and tiktok) ---")
    import pprint
    sample = db["song"].find_one({
        "chart_history": {"$exists": True},
        "tiktok": {"$exists": True}
    })
    if sample:
        pprint.pprint(sample)

    client.close()
    print("\nDone!")