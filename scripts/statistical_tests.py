import pandas as pd
from scipy.stats import kruskal
from scipy.stats import mannwhitneyu

# Load datasets
genre_df = pd.read_csv("results/tables/mongo_genre_weeks_on_chart.csv")
viral_df = pd.read_csv("results/tables/mongo_viral_weeks_on_chart.csv")

# ----------------------------------------------------------
# Research question 1: How does genre affect the average number
# of weeks a track remains on the Spotify Top 200 chart?
# ----------------------------------------------------------

# Group data by genre
groups = []
for genre in genre_df["genre"].unique():
    group = genre_df[genre_df["genre"] == genre]["weeks_on_chart"]
    if len(group) >= 3:  # filter small groups
        groups.append(group)

# Run Kruskal-Wallis test
h_stat_genre, p_value_genre = kruskal(*groups)

print("H-statistic:", h_stat_genre)
print("P-value:", p_value_genre)


# ----------------------------------------------------------
# Research question 2: Do songs that go viral on TikTok remain
# on the Spotify Top 200 chart for a longer duration compared 
# to non-viral songs?
# ----------------------------------------------------------

# Extract weeks on chart for viral and non-viral songs
viral = viral_df[viral_df["is_viral"] == "TikTok Viral"]["weeks_on_chart"]
non_viral = viral_df[viral_df["is_viral"] == "Non Viral"]["weeks_on_chart"]

# Run t-test using Mann-Whitney U test (non-parametric)
u_stat_viral, p_value_viral = mannwhitneyu(viral, non_viral, alternative='two-sided')

print("U statistic:", u_stat_viral)
print("P-value:", p_value_viral)


# Make a results table
results = [
    {
        "Research Question": "Genre effect on weeks on chart",
        "Test": "Kruskal-Wallis",
        "Statistic": round(h_stat_genre, 5),
        "P-value": p_value_genre
    },
    {
        "Research Question": "TikTok viral vs non-viral",
        "Test": "Mann-Whitney U",
        "Statistic": round(u_stat_viral, 5),
        "P-value": p_value_viral
    }
]

# Convert to DataFrame
results_df = pd.DataFrame(results)
# Save to CSV
results_df.to_csv("results/tables/statistical_test_results.csv", index=False)