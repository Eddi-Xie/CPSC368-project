# Analysis script for MongoDB data (step 4 of the Phase 4 instructions)

# Import necessary libraries
import pymongo
import pandas as pd
from scipy import stats

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

# Save results to a CSV file
df1 = pd.DataFrame(result1)
df1 = df1.rename(columns={"_id": "genre"})
df1.to_csv("results/tables/mongo_genre_avg_weeks.csv", index=False)

# --- One-way ANOVA: Genre vs Chart Longevity ---
# Retrieve individual song-level data (genre + weeks_on_chart) for the test
pipeline1_individual = [
    {"$match": {
        "chart_history": {"$exists": True},
        "genre": {"$ne": None}
    }},
    {"$project": {
        "genre": 1,
        "weeks_on_chart": {"$size": "$chart_history"}
    }}
]
result1_individual = list(db["song"].aggregate(pipeline1_individual))
df1_individual = pd.DataFrame(result1_individual)

# Split weeks_on_chart into separate arrays per genre
genre_groups = [group["weeks_on_chart"].values for _, group in df1_individual.groupby("genre")]

f_stat, p_value = stats.f_oneway(*genre_groups)
print(f"\nOne-way ANOVA (Genre vs Chart Longevity):")
print(f"  F-statistic: {f_stat:.4f}")
print(f"  p-value: {p_value:.4f}")
if p_value < 0.05:
    print("  Result: Statistically significant (p < 0.05) — genre has a significant effect on chart longevity.")
else:
    print("  Result: Not statistically significant (p >= 0.05) — no significant effect of genre on chart longevity.")

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

# Save results to a CSV file
df2 = pd.DataFrame(result2)
df2 = df2.rename(columns={"_id": "category"})
df2.to_csv("results/tables/mongo_tiktok_viral_avg_weeks.csv", index=False)

# --- Welch's t-test: TikTok Viral vs Non-Viral Chart Longevity ---
# Retrieve individual song-level data (viral status + weeks_on_chart) for the test
pipeline2_individual = [
    {"$match": {"chart_history": {"$exists": True}}},
    {"$project": {
        "weeks_on_chart": {"$size": "$chart_history"},
        "is_viral": {
            "$cond": {
                "if": {"$gte": [{"$ifNull": ["$tiktok.popularity", 0]}, 70]},
                "then": "TikTok Viral",
                "else": "Non Viral"
            }
        }
    }}
]
result2_individual = list(db["song"].aggregate(pipeline2_individual))
df2_individual = pd.DataFrame(result2_individual)

viral_weeks = df2_individual[df2_individual["is_viral"] == "TikTok Viral"]["weeks_on_chart"]
nonviral_weeks = df2_individual[df2_individual["is_viral"] == "Non Viral"]["weeks_on_chart"]

t_stat, p_value = stats.ttest_ind(viral_weeks, nonviral_weeks, equal_var=False)
print(f"\nWelch's t-test (TikTok Viral vs Non-Viral):")
print(f"  t-statistic: {t_stat:.4f}")
print(f"  p-value: {p_value:.4f}")
if p_value < 0.05:
    print("  Result: Statistically significant (p < 0.05) — TikTok viral songs have significantly different chart longevity.")
else:
    print("  Result: Not statistically significant (p >= 0.05) — no significant difference in chart longevity.")

client.close()
print("\nDone!")