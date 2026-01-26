import pandas as pd
from sklearn.metrics import f1_score

df = pd.read_csv("./validation/merged.csv")
bins = [
    (0.75, 0.85),
    (0.85, 0.90),
    (0.90, 1.01),  # upper bound slightly above 1 to include 1.0
]

results = []

for low, high in bins:
    subset = df[(df["Similarity"] >= low) & (df["Similarity"] < high)]

    if subset.empty:
        f1 = None
        count = 0
    else:
        y_true = subset["is_duplicated"]
        y_pred = [1] * len(subset)  # predicted positive within the bin
        f1 = f1_score(y_true, y_pred, zero_division=0)
        count = len(subset)

    results.append(
        {"similarity_range": f"[{low}, {high})", "num_samples": count, "f1_score": f1}
    )

# Save results to CSV
output_df = pd.DataFrame(results)
output_df.to_csv("f1_by_range.csv", index=False)

print(output_df)
