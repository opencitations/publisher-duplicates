import polars as pl


def check_null_crossrefs():
    id_pattern = r"\[(?:\w+:[^\]]+)(?:\s+\w+:[^\]]+)*\]"
    omid_pattern = r"omid:([^\]\s]+)"
    crossref_pattern = r"crossref:([^\]\s;,]+)"
    df = (
        pl.scan_parquet("./data/parquets/")
        .select(pl.col("publisher"))
        .drop_nulls()
        .with_columns(
            [
                # Extract first publisher entry and ID substring
                pl.col("publisher")
                .str.split("; ")
                .list.first()  # NOTE: Issue on openCitations Github is open about repeated entries. For now, first element is chosen
            ]
        )
        .with_columns(
            pl.col("publisher").str.extract(id_pattern, 0).alias("id_vals"),
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
                # Handle empty strings
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
        .select(["publisher", "literal", "pub_omid", "pub_cr"])
    )
    print(df.select(pl.len()).collect().item())


def main(input_path, output_path="./lit_dupes.csv"):

    df = pl.scan_parquet(input_path)
    print("Working with following data size:")
    print(df.select(pl.len()).collect().item())
    # print(df.head(20).collect())

    print("\nEntries with crossref:")
    entries_with_cr = df.filter(pl.col("pub_cr") != "")
    print("\t", entries_with_cr.select(pl.len()).collect().item())
    multiple_crs = entries_with_cr.filter(pl.col("pub_cr").str.contains(";"))
    print("Of which having multiple CrossRef IDs assigned:")
    print("\t", multiple_crs.select(pl.len()).collect().item())
    print(multiple_crs.collect())

    omid_duplicates = (
        df.group_by("pub_omid")
        .agg(pl.col("entry_id"), pl.col("pub_cr"), pl.col("literal"))
        .filter(
            (pl.col("entry_id").list.len() > 1) & (pl.col("pub_omid").is_not_null())
        )
        .with_columns(
            pl.col("entry_id").list.len().alias("num_entries"),
            pl.col("pub_cr").list.len().alias("num_omids"),
        )
        .sort("num_entries", descending=True)
    )
    # print(omid_duplicates.head(10).collect())
    print("OMID duplicate IDs:")
    print("\t", omid_duplicates.select(pl.len()).collect().item())

    cr_duplicates = (
        df.group_by("pub_cr")
        .agg(pl.col("entry_id"), pl.col("pub_omid"), pl.col("literal"))
        .filter((pl.col("entry_id").list.len() > 1) & (pl.col("pub_cr").is_not_null()))
        .with_columns(
            pl.col("entry_id").list.len().alias("num_entries"),
            pl.col("pub_omid").list.len().alias("num_omids"),
        )
        .sort("num_entries", descending=True)
    )
    print("CrossRef duplicate IDs:")
    # print(cr_duplicates.head(10).collect())
    print("\t", cr_duplicates.select(pl.len()).collect().item())

    lit_duplicates = (
        df.group_by("literal")
        .agg(
            [
                pl.col("entry_id"),
                pl.col("pub_cr"),
                pl.col("pub_omid"),
            ]
        )
        .filter(pl.col("entry_id").list.len() > 1)
        .with_columns(
            pl.col("entry_id").list.join("; "),
            pl.col("pub_cr").list.join("; "),
            pl.col("pub_omid").list.join("; "),
            pl.col("entry_id").list.len().alias("num_entries"),
            pl.col("pub_cr").list.len().alias("num_crs"),
        )
        .sort("num_entries", descending=True)
    )
    print("Identical denomination:")
    # print(lit_duplicates.head(10).collect())
    print("\t", lit_duplicates.select(pl.len()).collect().item())

    lit_duplicates.sink_csv(output_path)


if __name__ == "__main__":

    main(input_path="../data/processed_data.parquet")
