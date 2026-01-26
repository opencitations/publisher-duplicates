from typing import List

import pandas as pd

INPUT_PATH = ".../test_results/duplicates.csv"
THRESHOLDS = [0.75, 0.85, 0.90]
BEST_T = 0.90
SAMPLE_SIZE = 15

VAL_SPLITS = [
    "./validation/val_sample_0.75.csv",
    "./validation/val_sample_0.85.csv",
    "./validation/val_sample_0.9.csv",
]


def get_val_splits(df, existing: List[str] | None = None):
    if existing:
        return [pd.read_csv(f) for f in existing]
    val_samples = []
    start = 0
    for i, t in enumerate(THRESHOLDS):
        lower = t
        # last threshold should always have 1 as upper bound
        if i == len(THRESHOLDS) - 1:
            upper = 1
        else:
            upper = THRESHOLDS[i + 1]

        filtered = df[(df["Similarity"] >= lower) & (df["Similarity"] <= upper)]
        sample_df = filtered.iloc[start : 1 + start + SAMPLE_SIZE].reset_index(
            drop=True
        )
        sample_df = sample_df[
            ["From", "Similarity", "To", "entry_id_from", "entry_id_to"]
        ]
        sample_df.to_csv(f"./validation/val_sample_{t}.csv")
        val_samples.append(sample_df)
        start += SAMPLE_SIZE
    return val_samples


def main():
    data = (
        pd.read_csv(INPUT_PATH).sample(frac=1.0).reset_index(drop=True)
    )  # shuffling data
    val_samples = get_val_splits(data, existing=VAL_SPLITS)

    # performing anti-join to remove data used in validation
    used_dps = pd.concat(val_samples)
    test_dps = (
        data.merge(used_dps, how="left", indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns="_merge")
    )
    filtered_sample = test_dps[test_dps["Similarity"] >= BEST_T].sample(n=33)
    filtered_sample.to_csv("./test_set.csv")

    return


if __name__ == "__main__":
    main()
