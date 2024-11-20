# remove_bollinger_bool_columns.py

"""
This script iterates through all CSV files in the specified 'historical_data' folder
and removes the columns BB15_Bool, BB20_Bool, and BB25_Bool if they exist.

Usage:
    python remove_bollinger_bool_columns.py
"""

import os
import pandas as pd
import glob

def remove_columns_from_csv(file_path, columns_to_remove):
    """
    Removes specified columns from a CSV file if they exist.

    Parameters:
    - file_path (str): Path to the CSV file.
    - columns_to_remove (list): List of column names to remove.
    """
    try:
        # Load the CSV file
        df = pd.read_csv(file_path)

        # Identify columns to remove that exist in the DataFrame
        existing_columns = [col for col in columns_to_remove if col in df.columns]

        if not existing_columns:
            print(f"No specified columns found in {os.path.basename(file_path)}. Skipping.")
            return

        # Remove the columns
        df.drop(columns=existing_columns, inplace=True)

        # Save the modified DataFrame back to CSV
        df.to_csv(file_path, index=False)
        print(f"Removed columns {existing_columns} from {os.path.basename(file_path)}.")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def main():
    # Define the path to the historical_data folder
    # Update these variables based on your environment
    drive = "x:"
    folder = "historical_data"
    output_dir = os.path.join(drive, folder)

    # Verify that the directory exists
    if not os.path.exists(output_dir):
        print(f"Directory {output_dir} does not exist. Please check the path.")
        return

    # Define the columns to remove
    columns_to_remove = ['BB15_Bool', 'BB20_Bool', 'BB25_Bool']

    # Use glob to find all CSV files in the directory
    csv_files = glob.glob(os.path.join(output_dir, "*.csv"))

    if not csv_files:
        print(f"No CSV files found in {output_dir}.")
        return

    # Iterate through each CSV file and remove the specified columns
    for file_path in csv_files:
        remove_columns_from_csv(file_path, columns_to_remove)

if __name__ == "__main__":
    main()