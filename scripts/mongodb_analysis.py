# Analysis script for MongoDB data (step 4 of the Phase 4 instructions)

# Import necessary libraries
import pymongo

# IMPORTANT: UPDATE THESE BEFORE RUNNING
CWL = 'xxx'
SNUM = 'xxx'

# Check if CWL and SNUM have been updated from the placeholder values (from the 
# "Connecting to MongoDB via Python" Canvas page)
if CWL.strip() == "" or CWL == 'xxx' or SNUM.strip() == "" or SNUM == 'xxx':
    print("You need up to update the value of the CWL and/or SNUM variables before proceeding.")
else:
    # Connect to MongoDB
    connection_string = f"mongodb://{CWL}:a{SNUM}@localhost:27017/{CWL}"
    client = pymongo.MongoClient(connection_string)
    db = client[CWL]

    # ----------------------------------------------------------
    # Research question 1: How does genre affect the average number
    # of weeks a track remains on the Spotify Top 200 chart?
    # ----------------------------------------------------------

    pipeline1 = [
        # Only consider songs with chart history and genre (similar to the WHERE clause in SQL)
        {"$match": {
            "chart_history": {"$exists": True},
            "genre": {"$ne": None}
        }},
        # Project the genre and calculate weeks on chart 
        # (replaces the SELECT + calculation of weeks on chart in SQL)
        {"$project": {
            "genre": 1,
            "weeks_on_chart": {"$size": "$chart_history"}
        }},
        # Group by genre and calculate average weeks on chart (replaces the GROUP BY and AVG in SQL)
        {"$group": {
            "_id": "$genre",
            "avg_weeks_on_chart": {"$avg": "$weeks_on_chart"}
        }},
        # Sort by average weeks on chart in descending order (replaces the ORDER BY in SQL)
        {"$sort": {"avg_weeks_on_chart": -1}}
    ]

    # Execute the aggregation pipeline and convert results to a list
    result1 = list(db["song"].aggregate(pipeline1))
    print("Average Spotify Chart Longevity by Genre:")
    for row in result1:
        print(f"  {row['_id']}: {row['avg_weeks_on_chart']:.2f} weeks")

    # ----------------------------------------------------------
    # Research question 2: Do songs that go viral on TikTok remain
    # on the Spotify Top 200 chart for a longer duration compared 
    # to non-viral songs?
    # ----------------------------------------------------------

    pipeline2 = [
        # Only consider songs with chart history (similar to the WHERE clause in SQL)
        {"$match": {"chart_history": {"$exists": True}}},
        # Project weeks on chart and determine if the song is TikTok viral
        # (replaces the SELECT and CASE statement in SQL)
        {"$project": {
            "weeks_on_chart": {"$size": "$chart_history"},
            "is_viral": {
                "$cond": {
                    "if": {"$gte": [{"$ifNull": ["$tiktok.popularity", 0]}, 70]},
                    "then": "TikTok Viral",
                    "else": "Non Viral"
                }
            }
        }},
        # Group by viral status and calculate count of songs and average weeks on chart
        # (replaces the GROUP BY, COUNT, and AVG in SQL)
        {"$group": {
            "_id": "$is_viral",
            "num_songs": {"$sum": 1},
            "avg_weeks_on_chart": {"$avg": "$weeks_on_chart"}
        }},
        # Sort by viral status
        {"$sort": {"_id": 1}}
    ]

    # Execute the aggregation pipeline and convert results to a list
    result2 = list(db["song"].aggregate(pipeline2))
    print("\nTikTok Viral vs Non-Viral Songs:")
    for row in result2:
        print(f"  {row['_id']}: {row['num_songs']} songs, Average weeks on chart: {row['avg_weeks_on_chart']:.2f}")

    client.close()
    print("\nDone!")