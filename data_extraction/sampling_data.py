import polars as pl
import os
from tqdm import tqdm
import pandas as pd




parquet_directory = "../data/raw_data/French-PD-Books"  # directory downloaded from HuggingFace

# List all the Parquet files in the directory
parquet_files = [f for f in os.listdir(parquet_directory) if f.endswith(".parquet")]
print(len(parquet_files))


sampled_data = []
for file in tqdm(parquet_files):
    file_path = os.path.join(parquet_directory, file)
    df = pl.read_parquet(file_path)
    sampled_df = df.sample(200, seed=42)  # Sample 200 rows
    sampled_data.append(sampled_df)


# Concat and save the final sampled dataset
final_concat = pl.concat(sampled_data)
final_concat.write_parquet("../data/raw_data/gallica_mono_sampling.parquet")

df_title = final_concat.drop('complete_text', axis=1)
df_title.to_csv('../data/raw_data/galica_mon_sampling_title.csv')