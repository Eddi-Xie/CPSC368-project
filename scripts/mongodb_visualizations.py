# Analysis script for MongoDB data (step 4 of the Phase 4 instructions)

# Import necessary libraries
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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
    }}
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

# Strip plot of weeks on chart by genre
plt.figure()

# Create strip plot
plt.figure(figsize=(10, 6))
sns.stripplot(
    data=df1,
    x="genre",
    y="weeks_on_chart",
    order=genre_order,
    jitter=True,
    alpha=0.6)

# Show average weeks on chart for each genre using a point plot (without error bars)
sns.pointplot(
    data=df1,
    x="genre",
    y="weeks_on_chart",
    order=genre_order,
    color="red",
    markers="o",
    linestyles="",
    errorbar=None,
    label="Average Weeks on Chart"
)

# Customize plot and labels
plt.xticks(rotation=45, fontsize=12)
plt.xlabel("Genre", fontsize=14)
plt.ylabel("Weeks on Chart", fontsize=14)
plt.title("Distribution of Spotify Chart Longevity by Genre (with Averages)", fontsize=16)
plt.legend(loc="upper right")
plt.tight_layout()

# Save figure
plt.savefig("results/figures/mongo_genre_stripplot.png", dpi=300)
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
    }}
]

# Execute the aggregation pipeline and convert results to a list
result2 = list(db["song"].aggregate(pipeline_visualization2))

# Save results to a DataFrame and CSV
df2 = pd.DataFrame(result2)
df2.to_csv("results/tables/mongo_viral_weeks_on_chart.csv", index=False)

# Define colours for the histogram so that it is consistent with the average lines we will add to the plot later
palette = {
"TikTok Viral": "orange",
"Non Viral": "blue"}

# Histogram of weeks on chart by viral status
plt.figure()
sns.histplot(
    data=df2,
    x="weeks_on_chart",
    hue="is_viral",
    bins=20,
    alpha=0.5,
    multiple="layer",
    palette=palette
)

# Add vertical lines for the average weeks on chart for each group
means = df2.groupby("is_viral")["weeks_on_chart"].mean()

plt.axvline(means["TikTok Viral"], linestyle='dashed', linewidth=2, color="orange", label="Viral Avg")
plt.axvline(means["Non Viral"], linestyle='dashed', linewidth=2, color="blue", label="Non-Viral Avg")

# Customize plot and labels
plt.xlabel("Weeks on Chart")
plt.ylabel("Frequency")
plt.title("Distribution of Spotify Chart Longevity: TikTok Viral vs Non-Viral Songs")
plt.legend()
plt.tight_layout()

# Save figure
plt.savefig("results/figures/mongo_tiktok_histogram.png", dpi=300)
# Show figure
plt.show()

client.close()
print("\nDone!")