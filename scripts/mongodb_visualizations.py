# Analysis script for MongoDB data (step 4 of the Phase 4 instructions)

# Import necessary libraries
from turtle import lt

import pymongo
import pandas as pd
import matplotlib.pyplot as plt

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

    pipeline_visualization1 = [
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
        # Sort by average weeks on chart in descending order (replaces the ORDER BY in SQL)
        {"$sort": {"avg_weeks_on_chart": -1}}
    ]

    # Execute the aggregation pipeline and convert results to a list
    result1 = list(db["song"].aggregate(pipeline_visualization1))
    
    # Save results to a DataFrame and CSV
    df1 = pd.DataFrame(result1)
    df1 = df1[["genre", "weeks_on_chart"]]
    df1.to_csv("results/tables/mongo_genre_weeks_on_chart.csv", index=False)

    # Determine genre order based on average weeks on chart for better visualization
    genre_order = (
    df1.groupby("genre")["weeks_on_chart"]
    .mean()
    .sort_values(ascending=False)
    .index)

    # Convert genre to ordered categorical
    df1["genre"] = pd.Categorical(df1["genre"], categories=genre_order, ordered=True)

    # Boxplot of weeks on chart by genre
    plt.figure()
    df1.boxplot(column="weeks_on_chart", by="genre", rot=45)
    plt.title("Distribution of Weeks on Chart by Genre")
    plt.suptitle("")
    plt.xlabel("Genre")
    plt.ylabel("Weeks on Chart")
    plt.tight_layout()

    # Save figure
    plt.savefig("results/figures/mongo_genre_boxplot.png", dpi=300)
    # Show figure
    plt.show()

    # ----------------------------------------------------------
    # Research question 2: Do songs that go viral on TikTok remain
    # on the Spotify Top 200 chart for a longer duration compared 
    # to non-viral songs?
    # ----------------------------------------------------------

    pipeline_visualization2 = [
        # Only consider songs with chart history (similar to the WHERE clause in SQL)
        {"$match": {"chart_history": {"$exists": True}}},
        # Project weeks on chart and determine if the song is TikTok viral
        # (replaces the SELECT and CASE statement in SQL)
        {"$project": {
            "_id": 0,
            "weeks_on_chart": {"$size": "$chart_history"},
            "is_viral": {
                "$cond": {
                    "if": {"$gte": [{"$ifNull": ["$tiktok.popularity", 0]}, 70]},
                    "then": "TikTok Viral",
                    "else": "Non Viral"
                }
            }
        }},
        # Sort by viral status
        {"$sort": {"_id": 1}}
    ]

    # Execute the aggregation pipeline and convert results to a list
    result2 = list(db["song"].aggregate(pipeline_visualization2))
    
    # Save results to a DataFrame and CSV
    df2 = pd.DataFrame(result2)
    df2.to_csv("results/tables/mongo_viral_weeks_on_chart.csv", index=False)

    # Boxplot of weeks on chart by genre
    plt.figure()
    df2.boxplot(column="weeks_on_chart", by="is_viral")
    plt.title("Weeks on Chart: Viral vs Non-Viral Songs")
    plt.suptitle("")
    plt.ylabel("Weeks on Chart")
    plt.tight_layout()

    # Save figure
    plt.savefig("results/figures/mongo_tiktok_viral_boxplot.png", dpi=300)
    # Show figure
    plt.show()

    client.close()
    print("\nDone!")