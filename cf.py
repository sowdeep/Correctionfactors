import pandas as pd
import os
import re
import glob
import numpy as np # Import numpy for np.nan

# --- Configuration ---
# Set the main directory where this script is located.
# The script assumes it's running from: 'C:\\Users\\aaa\\Desktop\\correction factor of every station'
BASE_DIR = os.getcwd() # Gets the current working directory where the script is executed

# Define the sub-directory containing the satellite data files.
SATELLITE_DATA_DIR = os.path.join(BASE_DIR, 'satellite data readings')

# --- Output File Names (Currently set to CSV, can be changed to XLSX if preferred) ---
SATELLITE_AVG_OUTPUT_FILE = os.path.join(BASE_DIR, 'satellite_yearly_averages.csv')
YEARLY_CORRECTION_FACTOR_FILE = os.path.join(BASE_DIR, 'yearly_correction_factors.csv')
GRAND_CORRECTION_FACTOR_FILE = os.path.join(BASE_DIR, 'grand_correction_factors.csv')

# --- Helper Function for Saving DataFrames ---
# This helper can be extended to handle Excel if output format changes
def save_dataframe(df, file_path):
    """
    Saves a pandas DataFrame to the specified file path.
    Includes basic error handling for permissions.
    """
    try:
        # Check file extension to decide save method
        if file_path.lower().endswith('.csv'):
            df.to_csv(file_path, index=False)
        elif file_path.lower().endswith(('.xlsx', '.xls')):
            # Ensure openpyxl is installed for Excel support: pip install openpyxl
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
        else:
            print(f"Error: Unsupported output file type for {file_path}. Please use .csv or .xlsx.")
            return False
        
        print(f"Successfully saved data to: '{file_path}'")
        return True
    except PermissionError:
        print(f"ERROR: Permission denied when saving '{file_path}'.")
        print("Please ensure the file is not open in another program (like Excel/Notepad) and you have write permissions to the folder.")
        print("Try running Command Prompt as administrator.")
        return False
    except Exception as e:
        print(f"ERROR: An unexpected error occurred while saving '{file_path}': {e}")
        return False


def calculate_satellite_averages(data_directory):
    """
    Task 1: Reads all station files (CSV and Excel) from the satellite data
    directory, calculates the yearly average precipitation for each station,
    and returns a DataFrame with the results.
    """
    print("--- Task 1: Processing Satellite Data ---")
    
    # Find all .csv and .xlsx files in the specified directory
    search_path_csv = os.path.join(data_directory, '*.csv')
    search_path_xlsx = os.path.join(data_directory, '*.xlsx')
    all_files = glob.glob(search_path_csv) + glob.glob(search_path_xlsx)

    if not all_files:
        print(f"Error: No CSV or Excel files found in '{data_directory}'. Please check the path.")
        return pd.DataFrame()

    print(f"Found {len(all_files)} station files to process.")
    
    all_station_data = []

    for file_path in all_files:
        try:
            # Extract the station number from the filename (e.g., '806' from '806.csv')
            filename = os.path.basename(file_path)
            station_number_str = os.path.splitext(filename)[0].strip() # Strip any whitespace
            
            # Ensure station_number is treated as an integer
            if not station_number_str.isdigit():
                print(f"  - Skipping file with non-numeric name: {filename}")
                continue
            station_number = int(station_number_str)

            print(f"  - Reading file: {filename} for station {station_number}") # Debug print
            
            # Read the file, skipping the first 9 rows to get to the header at row 10
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path, header=9)
            else: # .xlsx or .xls
                df = pd.read_excel(file_path, header=9)

            # Ensure required columns exist
            required_cols = ['YEAR', 'MO', 'DY', 'PRECTOTCORR']
            # Strip whitespace from column names to ensure exact match
            df.columns = df.columns.str.strip() 
            if not all(col in df.columns for col in required_cols):
                print(f"    - Warning: Skipping {filename}. Missing one of the required columns: {required_cols}. Found: {df.columns.tolist()}")
                continue

            # Convert precipitation column to numeric, coercing errors to NaN
            df['PRECTOTCORR'] = pd.to_numeric(df['PRECTOTCORR'], errors='coerce')
            
            # Remove rows where precipitation data is invalid/missing
            df.dropna(subset=['PRECTOTCORR'], inplace=True)

            # Check if any data remains after dropping NaNs
            if df.empty:
                print(f"    - Warning: No valid PRECTOTCORR data found in {filename} after cleaning. Skipping.")
                continue

            # Calculate the yearly average for 'PRECTOTCORR'
            yearly_avg = df.groupby('YEAR')['PRECTOTCORR'].mean().reset_index()
            
            # Rename columns for consistency
            yearly_avg.rename(columns={'YEAR': 'Year', 'PRECTOTCORR': 'Satellite Average'}, inplace=True)
            
            # Add the station number to the dataframe
            yearly_avg['Station Number'] = station_number
            
            all_station_data.append(yearly_avg)

        except Exception as e:
            print(f"    - Error processing file {file_path}: {e}")

    if not all_station_data:
        print("No valid satellite data could be processed.")
        return pd.DataFrame()
        
    # Combine all dataframes into a single one
    final_df = pd.concat(all_station_data, ignore_index=True)
    
    # Reorder columns for clarity
    final_df = final_df[['Station Number', 'Year', 'Satellite Average']]
    
    # Save the result
    if save_dataframe(final_df, SATELLITE_AVG_OUTPUT_FILE):
        return final_df
    else:
        return pd.DataFrame() # Return empty if save failed


def read_observed_data(file_path):
    """
    Task 2: Reads the observed data file (CSV or Excel), extracts the station number from
    the 'File' column, and returns a clean DataFrame.
    """
    print("\n--- Task 2: Processing Observed Data ---")
    
    try:
        # Determine how to read the file based on its extension
        if file_path.lower().endswith('.csv'):
            df = pd.read_csv(file_path)
            print(f"Successfully read observed data from CSV file: {os.path.basename(file_path)}")
        elif file_path.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
            print(f"Successfully read observed data from Excel file: {os.path.basename(file_path)}")
        else:
            print(f"Error: Unsupported file type for observed data: {file_path}")
            return pd.DataFrame()

        print(f"  - Found {len(df)} rows in the observed data file.")

        # Function to extract the station number.
        # This regex finds the first number with 3 or more digits, which is more
        # likely to be the station ID than shorter numbers.
        def extract_station_number(file_name_string):
            if not isinstance(file_name_string, str):
                return None
            # Strip leading/trailing whitespace from the string before regex search
            cleaned_string = file_name_string.strip() 
            match = re.search(r'(\d{3,})', cleaned_string) # Look for numbers with 3 or more digits
            return int(match.group(0)) if match else None

        # Apply the function to create a 'Station Number' column
        df['Station Number'] = df['File'].apply(extract_station_number)
        
        valid_stations = df['Station Number'].notna().sum()
        print(f"  - Successfully extracted {valid_stations} station numbers.")
        if valid_stations == 0:
            print("  - WARNING: Could not extract any station numbers. Please check the 'File' column format in your observed data file.")
        
        # Drop rows where station number couldn't be extracted
        df.dropna(subset=['Station Number'], inplace=True)
        df['Station Number'] = df['Station Number'].astype(int)

        # Select and rename columns to match for merging
        df_observed = df[['Station Number', 'Year', 'Average Data']].copy()
        df_observed.rename(columns={'Average Data': 'Observed Average'}, inplace=True)
        
        return df_observed

    except Exception as e:
        print(f"Error processing observed data file {file_path}: {e}")
        return pd.DataFrame()


def main():
    """
    Main function to orchestrate all tasks:
    1. Calculate satellite yearly averages.
    2. Read observed data.
    3. Merge data and calculate yearly correction factors.
    4. Calculate grand correction factors.
    """
    print("Starting data processing for correction factors...\n")

    # Task 1: Get satellite data averages
    df_satellite = calculate_satellite_averages(SATELLITE_DATA_DIR)
    if df_satellite.empty:
        print("\nHalting script because satellite data processing failed.")
        return

    print(f"\nSatellite data collected for {df_satellite['Station Number'].nunique()} unique stations.") # Debug print
    print(f"Sample Satellite Station Numbers: {sorted(df_satellite['Station Number'].unique())[:10]}...") # Debug print
    print(f"Sample Satellite Years: {sorted(df_satellite['Year'].unique())[:10]}...") # Debug print


    # Task 2: Get observed data
    # Automatically find if the observed file is a .csv or .xlsx
    observed_csv_path = os.path.join(BASE_DIR, 'all_files_years_and_averages.csv')
    observed_xlsx_path = os.path.join(BASE_DIR, 'all_files_years_and_averages.xlsx')

    observed_file_to_read = None
    if os.path.exists(observed_csv_path):
        observed_file_to_read = observed_csv_path
    elif os.path.exists(observed_xlsx_path):
        observed_file_to_read = observed_xlsx_path
    
    if not observed_file_to_read:
        print("\nFATAL ERROR: Could not find 'all_files_years_and_averages.csv' or 'all_files_years_and_averages.xlsx'.")
        print("Please make sure the file exists in the correct directory.")
        print("Halting script.")
        return
        
    df_observed = read_observed_data(observed_file_to_read)
    if df_observed.empty:
        print("\nHalting script because observed data processing failed or produced no data.")
        return

    print(f"\nObserved data collected for {df_observed['Station Number'].nunique()} unique stations.") # Debug print
    print(f"Sample Observed Station Numbers: {sorted(df_observed['Station Number'].unique())[:10]}...") # Debug print
    print(f"Sample Observed Years: {sorted(df_observed['Year'].unique())[:10]}...") # Debug print


    # Task 3: Merge data and calculate yearly correction factors
    print("\n--- Task 3: Merging Data and Calculating Correction Factors ---")
    
    # Ensure 'Station Number' and 'Year' columns are of consistent integer type before merging
    # Using 'Int64' for nullable integer type to handle potential NaNs gracefully
    df_observed['Station Number'] = df_observed['Station Number'].astype('Int64')
    df_observed['Year'] = df_observed['Year'].astype('Int64')
    df_satellite['Station Number'] = df_satellite['Station Number'].astype('Int64')
    df_satellite['Year'] = df_satellite['Year'].astype('Int64')

    df_merged = pd.merge(df_observed, df_satellite, on=['Station Number', 'Year'], how='inner')

    print(f"Shape of merged_df after inner merge: {df_merged.shape}") # Debug print

    if df_merged.empty:
        print("\nWARNING: No matching station-year pairs found between observed and satellite data.")
        print("No correction factors will be calculated. Check if station numbers and years overlap between your data sources.")
        return
        
    # Calculate the correction factor
    # Use np.nan for division by zero to indicate undefined values
    df_merged['Correction Factor'] = df_merged.apply(
        lambda row: row['Observed Average'] / row['Satellite Average'] if row['Satellite Average'] != 0 else np.nan,
        axis=1
    )
    
    # Save the yearly correction factors
    if not save_dataframe(df_merged, YEARLY_CORRECTION_FACTOR_FILE):
        return # Halt if saving fails
    
    # Task 4: Calculate and save the Grand Correction Factor
    print("\n--- Task 4: Calculating Grand Correction Factor ---")
    
    # Drop rows where Correction Factor is NaN (e.g., from division by zero) before calculating mean
    df_grand_factor = df_merged.dropna(subset=['Correction Factor'])

    if df_grand_factor.empty:
        print("No valid correction factors to calculate Grand Correction Factor after removing NaNs.")
        return

    df_grand_factor = df_grand_factor.groupby('Station Number')['Correction Factor'].mean().reset_index()
    df_grand_factor.rename(columns={'Correction Factor': 'Grand Correction Factor'}, inplace=True)
    
    # Save the grand correction factors
    if not save_dataframe(df_grand_factor, GRAND_CORRECTION_FACTOR_FILE):
        return # Halt if saving fails
    
    print("\n\nAll tasks completed successfully!")


if __name__ == '__main__':
    main()
