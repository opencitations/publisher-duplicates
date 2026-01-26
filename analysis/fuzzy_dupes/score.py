import pandas as pd
from sklearn.metrics import precision_score

input_path = input("Enter an input path: ")
df = pd.read_csv(input_path)
bins = [
    (0.75, 0.85),
    (0.85, 0.90),
    (0.90, 1.01),  # upper bound slightly above 1 to include 1.0
]

results = []

for low, high in bins:
    subset = df[(df["Similarity"] >= low) & (df["Similarity"] < high)]

    if subset.empty:
        precision, recall, f1 = None, None, None
        count = 0
    else:
        y_true = subset["is_duplicated"]
        y_pred = [1] * len(subset)  # predicted positive within the bin
        precision = precision_score(y_true, y_pred)
        count = len(subset)

    results.append(
        {
            "similarity_range": f"[{low}, {high})",
            "num_samples": count,
            "precision": precision,
        }
    )

# Save results to CSV
output_df = pd.DataFrame(results)
output_df.to_csv("report.csv", index=False)

print(output_df)
