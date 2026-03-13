import os
from dotenv import load_dotenv
import oracledb
import pandas as pd
import matplotlib.pyplot as plt

# Load environment variables from .env
load_dotenv()

# Read credentials
user = os.getenv("ORACLE_USER")
password = os.getenv("ORACLE_PASS")
host = os.getenv("ORACLE_HOST")
port = os.getenv("ORACLE_PORT")
service = os.getenv("ORACLE_SERVICE")

# Create DSN 
dsn = oracledb.makedsn(host, port, service_name=service)

# Connect to Oracle
connection = oracledb.connect(
    user=user,
    password=password,
    dsn=dsn)

cursor = connection.cursor()

# Research Question 1
# Genre vs Chart Longevity

cursor.execute("""
CREATE OR REPLACE VIEW SongWeeks AS
SELECT song_id, COUNT(week) AS weeks_on_chart
FROM TopCharts
GROUP BY song_id
""")

query1 = """
SELECT g.genre_name,
       AVG(sw.weeks_on_chart) AS avg_weeks_on_chart
FROM Genre g
JOIN Song s ON g.genre_id = s.genre_id
JOIN SongWeeks sw ON s.song_id = sw.song_id
GROUP BY g.genre_name
ORDER BY avg_weeks_on_chart DESC
"""

cursor.execute(query1)
rows = cursor.fetchall()

df1 = pd.DataFrame(rows, columns=["Genre", "Average Weeks"])

plt.figure()
plt.bar(df1["Genre"], df1["Average Weeks"])
plt.xlabel("Genre")
plt.ylabel("Average Weeks on Chart")
plt.title("Average Spotify Chart Longevity by Genre")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Research Question 2
# TikTok Viral Songs vs Non-Viral Songs

query2 = """
SELECT COUNT(*) AS num_viral_songs, AVG(sw.weeks_on_chart) AS avg_weeks_viral
FROM SongWeeks sw
JOIN TiktokTrending t ON sw.song_id = t.song_id
WHERE t.tiktok_popularity >= 70
"""
cursor.execute(query2)
viral_result = cursor.fetchone()

query3 = """
SELECT COUNT(*) AS num_nonviral_songs, AVG(sw.weeks_on_chart) AS avg_weeks_nonviral
FROM SongWeeks sw
JOIN Song s ON sw.song_id = s.song_id
WHERE s.song_id NOT IN (
    SELECT song_id
    FROM TiktokTrending
    WHERE tiktok_popularity >= 70
)
"""

cursor.execute(query3)
nonviral_result = cursor.fetchone()

viral_avg = viral_result[1]
nonviral_avg = nonviral_result[1]

labels = ["TikTok Viral", "Non Viral"]
values = [viral_avg, nonviral_avg]

plt.figure()
plt.bar(labels, values)
plt.ylabel("Average Weeks on Chart")
plt.title("Spotify Chart Longevity: TikTok Viral vs Non-Viral Songs")
plt.tight_layout()
plt.show()

cursor.close()
connection.close()