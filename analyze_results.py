"""
Description
Extracts aggregated data (joined weather + fire stats) from postgresql db into pandas 
Performs three types of analyses:
1. Pearson correlation heatmap between weather factors and fire variables
2. Regional comparison of fire radiative power severity using boxplots
3. Random forest regressor feature importance model
Saves resulting plots in png format inside figures/ folder
"""

import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sqlalchemy import create_engine

# Connect to PostgreSQL
DB_URI = "postgresql://cariappa:D3chamma@localhost:5433/fireweather"
engine = create_engine(DB_URI)

print(" Fetching joined fire-weather dataset from PostgreSQL")
df = pd.read_sql("SELECT * FROM fire_weather_joined", con=engine)
print(f"Loaded {len(df)} records across {df['region'].nunique()} regions.")

# Ensuring output directory for figures exists
os.makedirs("figures", exist_ok=True)
sns.set_theme(style="whitegrid")

# Analysis 1: Meteorological Correlation Map

print("\nRunning Analysis 1: Weather-Fire Metric Correlation Matrix")
corr_cols = [
    "detection_count",
    "total_frp",
    "temp_max",
    "temp_min",
    "precipitation",
    "wind_max",
    "humidity",
]
corr_matrix = df[corr_cols].corr()

plt.figure(figsize=(8, 6))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Correlation Matrix: Fire Detections vs. Meteorological Variables")
plt.tight_layout()
plt.savefig("figures/analysis1_correlation_matrix.png", dpi=300)
plt.close()
print("  Saved: figures/analysis1_correlation_matrix.png")


# Analysis 2: Regional Fire Radiative Power (FRP) Comparison

print(
    "\nRunning Analysis 2: Cross-Regional Fire Radiative Power (FRP) Comparison"
)
plt.figure(figsize=(9, 5))
sns.boxplot(data=df, x="region", y="total_frp", palette="YlOrRd_r")
plt.title("Fire Radiative Power (FRP) Distribution Across Regions")
plt.xlabel("Geographic Region")
plt.ylabel("Total FRP (MW)")
plt.yscale("log")  # applying Log scale due to skewed FRP intensity distributions
plt.tight_layout()
plt.savefig("figures/analysis2_regional_frp_comparison.png", dpi=300)
plt.close()

regional_stats = (
    df.groupby("region")[["total_frp", "detection_count"]].mean().reset_index()
)
print("  Regional Averages:")
print(regional_stats.to_string(index=False))
print("  Saved: figures/analysis2_regional_frp_comparison.png")


# Analysis 3: Feature Importance Analysis for Predicting Fire Radiative Power

print("\nRunning Analysis 3: Random Forest Feature Importance Analysis")
features = ["temp_max", "temp_min", "precipitation", "wind_max", "humidity"]
X = df[features].fillna(0)
y = df["total_frp"]

rf = RandomForestRegressor(n_estimators=100, random_state=42)
rf.fit(X, y)

importance_df = pd.DataFrame(
    {"Feature": features, "Importance": rf.feature_importances_}
).sort_values(by="Importance", ascending=False)

plt.figure(figsize=(8, 4.5))
sns.barplot(
    data=importance_df, x="Importance", y="Feature", palette="viridis"
)
plt.title("Random Forest Feature Importance for Fire Intensity (FRP)")
plt.xlabel("Relative Importance")
plt.ylabel("Meteorological Feature")
plt.tight_layout()
plt.savefig("figures/analysis3_feature_importance.png", dpi=300)
plt.close()

print("  Feature Importances:")
print(importance_df.to_string(index=False))
print("  Saved: figures/analysis3_feature_importance.png")

print("\nALL 3 FOLLOW-UP ANALYSES COMPLETED")