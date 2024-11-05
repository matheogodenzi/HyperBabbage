# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 17:10:37 2024

@author: matheo
"""

from rdkit import Chem
import pandas as pd
import csv
import time

#%%

def process_large_tsv_in_chunks(file_path, chunk_size=10000):
    # Record the start time
    start_time = time.time()

    # Initialize an empty DataFrame to collect chunks
    df_list = []
    max_columns = 0

    # Process the file in chunks
    with open(file_path, 'r', encoding='ISO-8859-1') as f:
        chunk = []
        for i, line in enumerate(f):
            # Split each line by tabs and strip newlines
            row = line.strip().split('\t')
            chunk.append(row)

            # Keep track of the maximum number of columns
            if len(row) > max_columns:
                max_columns = len(row)

            # Once chunk reaches the specified size, process it
            if len(chunk) == chunk_size:
                # Add padding to each row in the chunk and convert it to a DataFrame
                df_chunk = pd.DataFrame([r + [None] * (max_columns - len(r)) for r in chunk])
                df_list.append(df_chunk)
                chunk = []  # Reset the chunk

            # Print progress for large files
            if i % 100000 == 0:
                print(f'Processing line {i}')

        # Process the final chunk if there are remaining rows
        if chunk:
            df_chunk = pd.DataFrame([r + [None] * (max_columns - len(r)) for r in chunk])
            df_list.append(df_chunk)

    # Concatenate all the chunks into one DataFrame
    df = pd.concat(df_list, ignore_index=True)

    # Record the end time
    end_time = time.time()

    # Calculate and print the processing time
    processing_time = end_time - start_time
    print(f"Time taken to process the file in chunks: {processing_time:.2f} seconds")

    return df

# Use the function to read and process the TSV file
df = process_large_tsv_in_chunks('../BindingDB_All.tsv', chunk_size=10000)

# Optionally show a few rows or save the DataFrame
print(df.head())


#%% 

print(df.columns)

