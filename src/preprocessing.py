import os
from datetime import datetime
from pathlib import Path

import polars as pl


def process_data(input_path, output_dir: str | Path = "./data"):

    print(f"Starting data processing at {datetime.now().strftime('%H:%M:%S')}")

    # Pre-compile regex patterns
    id_pattern = r"\[(?:\w+:[^\]]+)(?:\s+\w+:[^\]]+)*\]"
    omid_pattern = r"omid:([^\]\s]+)"
    crossref_pattern = r"crossref:([^\]\s]+)"

    processed = (
        pl.scan_parquet(input_path)
        .drop_nulls(subset="publisher")
        .with_columns(
            [
                # Extract first publisher entry and ID substring
                pl.col("publisher")
                .str.split("; ")
                .list.first()  # NOTE: Issue on openCitations Github is open about repeated entries. For now, first element is chosen
                .alias("publisher")
            ]
        )
        .with_columns(
            [
                # Extract first publisher entry and ID substring
                pl.col("publisher")
                .str.extract(id_pattern, 0)
                .alias("id_vals"),
            ]
        )
        .with_columns(
            [
                # Extract literal by removing ID substring
                pl.col("publisher")
                .str.replace(id_pattern, "")
                .str.strip_chars(" [];")
                .str.normalize(form="NFKD")
                .alias("literal"),
                # Extract Omid ID
                pl.col("id_vals").str.extract(omid_pattern).alias("pub_omid"),
                # Extract CrossRef IDs
                pl.col("id_vals")
                .str.extract_all(crossref_pattern)
                .list.join("; ")
                .str.replace_all("crossref:", "")
                .alias("pub_cr"),
            ]
        )
        .with_columns(
            [
                # Handle empty strings efficiently
                pl.when(pl.col("pub_cr").str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col("pub_cr"))
                .alias("pub_cr"),
                pl.when(pl.col("pub_omid").str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col("pub_omid"))
                .alias("pub_omid"),
            ]
        )
        .select(["entry_id", "literal", "pub_omid", "pub_cr"])
    )

    print(processed.head(10).collect())
    print(processed.select(pl.len()).collect().item())

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    temp_path = os.path.join(output_dir, "temp_processed.parquet")
    final_path = os.path.join(output_dir, "processed_data.parquet")

    # stream df to temporary file (no deduplication yet)
    print("Step 1: Processing and writing to temporary file...")
    processed.sink_parquet(
        temp_path,
        compression="zstd",
        row_group_size=500_000,  # Smaller row groups for memory safety
        maintain_order=False,
    )

    # Step 2: Deduplicate from the temporary file
    print("Step 2: Deduplicating...")
    (
        pl.scan_parquet(temp_path)
        .unique(subset=["literal", "pub_omid", "pub_cr"], maintain_order=False)
        .sink_parquet(
            final_path,
            compression="zstd",
            row_group_size=500_000,
            maintain_order=False,
        )
    )

    # Clean up temporary file
    os.remove(temp_path)

    print(f"Finished data processing at {datetime.now().strftime('%H:%M:%S')}")
    return final_path


if __name__ == "__main__":
    INPUT_PATH: str = "./data/parquets/"
    process_data(INPUT_PATH)
