import csv
from datetime import datetime
from pathlib import Path

# ---------- paths ----------
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "processed_data"
OUTPUT_FILE = ROOT / "sql" / "phase_three_load.sql"

topcharts_file = DATA_DIR / "clean_spotify_top_200.csv"
tiktok_file = DATA_DIR / "clean_tiktok.csv"
spotify_file = DATA_DIR / "clean_spotify_track.csv"


# ---------- helper functions ----------
def load_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def clean_text(value):
    if value is None:
        return ""
    return str(value).strip()


def normalize_text(value):
    return clean_text(value).lower()


def is_missing(value):
    return clean_text(value) == ""


def sql_string(value):
    if is_missing(value):
        return "NULL"
    value = str(value).replace("'", "''")
    return f"'{value}'"


def sql_date(value):
    if is_missing(value):
        return "NULL"
    return f"DATE '{clean_text(value)}'"


def make_song_key(title, artist):
    return (normalize_text(title), normalize_text(artist))


def require_text(value, field_name):
    value = clean_text(value)
    if value == "":
        raise ValueError(f"Missing {field_name}")
    return value


def require_date(value, field_name):
    value = require_text(value, field_name)
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid {field_name}: {value}")
    return value


def parse_int(value, field_name):
    value = require_text(value, field_name)
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Invalid {field_name}: {value}")


def check_length(value, max_len, field_name):
    if not is_missing(value) and len(clean_text(value)) > max_len:
        raise ValueError(f"{field_name} is longer than {max_len} characters: {value}")


def drop_table_block(table_name):
    return [
        "BEGIN",
        f"  EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS';",
        "EXCEPTION",
        "  WHEN OTHERS THEN",
        "    IF SQLCODE != -942 THEN",
        "      RAISE;",
        "    END IF;",
        "END;",
        "/",
    ]


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


# ---------- load data ----------
top_df = load_csv(topcharts_file)
tiktok_df = load_csv(tiktok_file)
spotify_df = load_csv(spotify_file)

sql_lines = ["SET DEFINE OFF;", ""]

# ---------- drop/create ----------
for table_name in ["TiktokTrending", "TopCharts", "Song", "Genre", "Artist"]:
    sql_lines += drop_table_block(table_name)
    sql_lines.append("")

sql_lines += [
    "PURGE RECYCLEBIN;",
    "",
    "CREATE TABLE Artist (",
    "    artist_id NUMBER PRIMARY KEY,",
    "    artist_name VARCHAR2(200) NOT NULL UNIQUE",
    ");",
    "",
    "CREATE TABLE Genre (",
    "    genre_id NUMBER PRIMARY KEY,",
    "    genre_name VARCHAR2(50) NOT NULL UNIQUE",
    ");",
    "",
    "CREATE TABLE Song (",
    "    song_id NUMBER PRIMARY KEY,",
    "    artist_id NUMBER NOT NULL,",
    "    track_name VARCHAR2(200) NOT NULL,",
    "    canonical_title VARCHAR2(200) NOT NULL,",
    "    release_date DATE NOT NULL,",
    "    spotify_track_id VARCHAR2(22),",
    "    genre_id NUMBER,",
    "    FOREIGN KEY (artist_id) REFERENCES Artist(artist_id),",
    "    FOREIGN KEY (genre_id) REFERENCES Genre(genre_id)",
    ");",
    "",
    "CREATE TABLE TopCharts (",
    "    song_id NUMBER NOT NULL,",
    "    week DATE NOT NULL,",
    "    rank NUMBER NOT NULL,",
    "    PRIMARY KEY (song_id, week),",
    "    FOREIGN KEY (song_id) REFERENCES Song(song_id)",
    ");",
    "",
    "CREATE TABLE TiktokTrending (",
    "    song_id NUMBER PRIMARY KEY,",
    "    tiktok_popularity NUMBER NOT NULL,",
    "    tiktok_release_date DATE NOT NULL,",
    "    FOREIGN KEY (song_id) REFERENCES Song(song_id)",
    ");",
    ""
]

# ---------- ID tracking ----------
artist_ids = {}
genre_ids = {}
song_ids = {}

next_artist_id = 1
next_genre_id = 1
next_song_id = 1

# ---------- build songs from top charts + tiktok ----------
all_rows = []

for row in top_df:
    all_rows.append({
        "canonical_title": require_text(row["canonical_title"], "Song canonical_title"),
        "main_artist": require_text(row["main_artist"], "Song main_artist"),
        "track_name": require_text(row["track_name"], "Song track_name"),
        "release_date": require_date(row["release_date"], "Song release_date"),
        "spotify_track_id": clean_text(row.get("track_id"))
    })

for row in tiktok_df:
    all_rows.append({
        "canonical_title": require_text(row["canonical_title"], "Song canonical_title"),
        "main_artist": require_text(row["main_artist"], "Song main_artist"),
        "track_name": require_text(row["track_name"], "Song track_name"),
        "release_date": require_date(row["release_date"], "Song release_date"),
        "spotify_track_id": clean_text(row.get("track_id"))
    })

# remove duplicate songs
unique_songs = {}
for row in all_rows:
    key = make_song_key(row["canonical_title"], row["main_artist"])
    if key not in unique_songs or should_replace_song(unique_songs[key], row):
        unique_songs[key] = row

# build lookup for genres from spotify track file
genre_lookup = {}
for row in spotify_df:
    key = make_song_key(row["canonical_title"], row["main_artist"])
    genre_lookup[key] = normalize_text(row["track_genre"])

# ---------- Artist inserts ----------
artist_names = sorted({row["main_artist"] for row in unique_songs.values()})
for artist in artist_names:
    check_length(artist, 200, "artist_name")
    artist_ids[artist] = next_artist_id
    sql_lines.append(
        f"INSERT INTO Artist VALUES ({next_artist_id}, {sql_string(artist)});"
    )
    next_artist_id += 1

sql_lines.append("")

# ---------- Genre inserts ----------
genre_names = sorted({
    genre_lookup[key]
    for key in unique_songs
    if key in genre_lookup and not is_missing(genre_lookup[key])
})
for genre in genre_names:
    check_length(genre, 50, "genre_name")
    genre_ids[genre] = next_genre_id
    sql_lines.append(
        f"INSERT INTO Genre VALUES ({next_genre_id}, {sql_string(genre)});"
    )
    next_genre_id += 1

sql_lines.append("")

# ---------- Song inserts ----------
for key, row in unique_songs.items():
    artist_name = row["main_artist"]
    genre_name = genre_lookup.get(key)

    if genre_name in genre_ids:
        genre_id = genre_ids[genre_name]
    else:
        genre_id = "NULL"

    check_length(row["track_name"], 200, "track_name")
    check_length(row["canonical_title"], 200, "canonical_title")
    check_length(row["spotify_track_id"], 22, "spotify_track_id")

    song_ids[key] = next_song_id

    sql_lines.append(
        "INSERT INTO Song VALUES ("
        f"{next_song_id}, "
        f"{artist_ids[artist_name]}, "
        f"{sql_string(row['track_name'])}, "
        f"{sql_string(row['canonical_title'])}, "
        f"{sql_date(row['release_date'])}, "
        f"{sql_string(row['spotify_track_id'])}, "
        f"{genre_id}"
        ");"
    )
    next_song_id += 1

sql_lines.append("")

# ---------- TopCharts inserts ----------
best_chart_rows = {}

for row in top_df:
    key = make_song_key(row["canonical_title"], row["main_artist"])
    song_id = song_ids[key]
    week = require_date(row["week"], f"TopCharts week for {key}")
    rank = parse_int(row["rank"], f"TopCharts rank for {key}")

    chart_key = (song_id, week)
    if chart_key not in best_chart_rows or rank < best_chart_rows[chart_key]:
        best_chart_rows[chart_key] = rank

for song_id, week in sorted(best_chart_rows, key=lambda x: (x[1], x[0])):
    rank = best_chart_rows[(song_id, week)]
    sql_lines.append(
        f"INSERT INTO TopCharts VALUES ({song_id}, {sql_date(week)}, {rank});"
    )

sql_lines.append("")

# ---------- TiktokTrending inserts ----------
best_tiktok_rows = {}

for row in tiktok_df:
    key = make_song_key(row["canonical_title"], row["main_artist"])
    song_id = song_ids[key]
    popularity = parse_int(row["popularity"], f"Tiktok popularity for {key}")
    release_date = require_date(row["release_date"], f"Tiktok release_date for {key}")

    if song_id not in best_tiktok_rows or popularity > best_tiktok_rows[song_id]["popularity"]:
        best_tiktok_rows[song_id] = {
            "popularity": popularity,
            "release_date": release_date
        }

for song_id in sorted(best_tiktok_rows):
    row = best_tiktok_rows[song_id]
    sql_lines.append(
        "INSERT INTO TiktokTrending VALUES ("
        f"{song_id}, "
        f"{row['popularity']}, "
        f"{sql_date(row['release_date'])}"
        ");"
    )

sql_lines.append("")
sql_lines.append("COMMIT;")

# ---------- write file ----------
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(sql_lines) + "\n")

print("SQL file written to", OUTPUT_FILE)
