# Publisher Deduplication Tool for OpenCitationsMeta

This project is a pipeline designed to address the challenge of duplicate publisher entries in the OpenCitationsMeta data dumps. Publisher names often appear in multiple forms due to typographical errors, inconsistent metadata, or varying conventions. This tool provides a modular pipeline for cleaning, clustering, and deduplicating publisher names.
Experiments were conducted on the whole OpenCitations dumo, albeit an older version (13-02-2025). 

## Project Structure and Components

The codebase is organized into several directories, each serving a specific role in the deduplication workflow. The `src/` directory contains the codebase for data loading, preprocessing, clustering, and orchestration. The `analysis/` directory provides scripts for data exploration and validation on the results obtained from the pipeline. Data files, both raw and processed, are stored in the `data/` directory, while results and validation output are saved in `test_results/`.

### Data Loading and Preprocessing

The deduplication process begins with the extraction and normalization of publisher data provided as compressed CSV files within a tar archive. The `dataloader.py` script is responsible for extracting these files in batches, reading publisher records and extracting the necessary data: 
* entry IDs: both internal IDs like **omid** (which stands for OpenCitationsMeta Identifier) and external ones (such as **doi** etc.) which identify a bibliographic resource;
* publisher strings: strings representing the **name of the editor**;
* publisher IDs: **CrossRef** and **omid** identifiers assigned to publishers.

Once extracted, the data is passed to `preprocessing.py`, which performs several cleaning, leveraging the Polars string API. 
Publisher strings are split to select the first entry, and regular expressions are used to extract identifiers such as OMID and CrossRef IDs. The script also normalizes publisher names by using NFKD normalization, in order to help with the downstream task of normalization. The output is a parquet file containing cleaned publisher names and their associated identifiers.

### Data Exploration

Before deduplication, it is important to understand the extent and nature of duplication in the dataset. The `analysis/exploration.py` script provides utilities for this purpose. It loads the processed data and computes statistics such as the number of entries with and without CrossRef IDs, the frequency of duplicate OMID and CrossRef IDs, and the prevalence of identical or highly similar publisher names. These insights inform the choice of clustering and matching strategies.

### Clustering and Fuzzy Matching

The core deduplication logic is implemented in `clustering.py` and orchestrated by `main.py`. Publisher names are first embedded using a sentence transformer model (`all-MiniLM-L6-v2`), which captures semantic similarity between names. If a compatible GPU is available, the code leverages GPU-accelerated libraries for faster computation. The embeddings are then reduced in dimensionality using [UMAP]()https://umap-learn.readthedocs.io/en/latest/how_umap_works.html, and [HDBSCAN](https://hdbscan.readthedocs.io/en/latest/how_hdbscan_works.html) is applied to cluster similar publisher names. Each cluster is assigned a label, with `-1` indicating unclustered data. Clusters are paramount in ensuring a scalable 

Within each cluster, the tool applies PolyFuzz, a fuzzy string matching library based on **TF-IDF** n-grams, to compute pairwise similarity scores between publisher names. Pairs exceeding a configurable similarity threshold are considered potential duplicates. The results are saved for further validation.

### Validation and Evaluation

Manual validation is a critical step in the deduplication workflow. The `analysis/fuzzy_dupes/get_samples.py` script samples pairs of publisher names at different similarity thresholds and saves them as validation sets. This allows users to manually inspect and label pairs as true or false duplicates. The validation sets are carefully constructed to avoid overlap with the test set, ensuring unbiased evaluation.

The `analysis/fuzzy_dupes/score.py` script computes **precision** for deduplication at each similarity threshold, based on the manual labels. This feedback loop enables tuning of the similarity threshold and assessment of the deduplication quality on different levels.

## Running the Pipeline

To run the deduplication pipeline, follow these steps:

1. **Prepare the Data**: Place the raw tar archive containing publisher CSV files in the project directory. Update the `TAR_PATH` variable in `main.py` to point to this file.

2. **Extract and Preprocess**: Run the main script to extract, clean, and normalize the data. This will generate a processed parquet file in the specified output directory. Example command:

   ```bash
   python src/main.py
   ```

   The script will create necessary directories, extract the data, and process it through the cleaning pipeline.

3. **Clustering and Fuzzy Matching**: The main script will then embed publisher names, cluster them, and compute fuzzy similarity scores within each cluster. The results, including potential duplicate pairs, will be saved in the `test_results/` directory.

4. **Manual Validation**: Use the scripts in `analysis/fuzzy_dupes/` to sample pairs for manual validation. Open the generated CSV files and label each pair as a true or false duplicate. Save your labels in the provided format.

5. **Evaluation**: Run the scoring script to compute precision and F1 scores at different similarity thresholds. This will help you assess the effectiveness of the deduplication process and adjust parameters as needed.

## Implementation Details

The pipeline is implemented using modern Python data science libraries. `requirements.txt` contains the needed requirements to run the code.
Here are some of the core libraries used for implementing the pipeline:
* Polars is used for fast, memory-efficient data processing;
* sentence-transformers and UMAP provide scalable embedding and dimensionality reduction;
* HDBSCAN is chosen for its ability to find clusters of varying density, which is important given the diversity of publisher names;
* PolyFuzz enables efficient fuzzy matching within clusters.

The code is modular, with each stage encapsulated in its own script. Intermediate results are saved in standardized formats (parquet, CSV) to facilitate reproducibility and debugging. The pipeline is designed to be extensible, allowing for the integration of new models or matching strategies as needed.


## Conclusion

Results show a precision of **0.87** when using the best similarity threshold tuned on the validation samples, namely 0.90. 
In this range of similarity, most differences between strings come from abbreviations included in the string, stop words or punctuation.

Based on the experiments, a consistent part of the false duplicates stem faulty publisher names within the OCM data.
Other limitations were given by the problem afflicting repeated publisher IDs within the dump data, as described in this [issue](https://github.com/opencitations/oc_meta/issues/43).
