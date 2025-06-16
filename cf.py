import pandas as pd
import os
import re
import glob

# --- Configuration ---
# Set the main directory where this script is located.
# The script assumes it's running from: 'C:\\Users\\aaa\\Desktop\\correction factor'
BASE_DIR = os.getcwd() 

# Define the sub-directory containing the satellite data files.
SATELLITE_DATA_DIR = os.path.join(BASE_DIR, 'satellite data readings')

# --- Output File Names ---
SATELLITE_AVG_OUTPUT_FILE = 'satellite_yearly_averages.csv'
YEARLY_CORRECTION_FACTOR_FILE = 'yearly_correction_factors.csv'
GRAND_CORRECTION_FACTOR_FILE = 'grand_correction_factors.csv'


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
            station_number = os.path.splitext(filename)[0]
            
            # Ensure station_number is treated as an integer
            if not station_number.isdigit():
                print(f"  - Skipping file with non-numeric name: {filename}")
                continue
            station_number = int(station_number)

            print(f"  - Reading file: {filename} for station {station_number}")

            # Read the file, skipping the first 9 rows to get to the header at row 10
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path, header=9)
            else:
                df = pd.read_excel(file_path, header=9)

            # Ensure required columns exist
            required_cols = ['YEAR', 'MO', 'DY', 'PRECTOTCORR']
            if not all(col in df.columns for col in required_cols):
                print(f"    - Warning: Skipping {filename}. Missing one of the required columns: {required_cols}")
                continue

            # Convert precipitation column to numeric, coercing errors to NaN
            df['PRECTOTCORR'] = pd.to_numeric(df['PRECTOTCORR'], errors='coerce')
            
            # Remove rows where precipitation data is invalid/missing
            df.dropna(subset=['PRECTOTCORR'], inplace=True)

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
    
    # Save the result to a CSV file
    final_df.to_csv(SATELLITE_AVG_OUTPUT_FILE, index=False)
    print(f"\nSuccessfully created '{SATELLITE_AVG_OUTPUT_FILE}' with satellite yearly averages.")
    
    return final_df


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
            # *** FIX: Look for numbers with 3 or more digits to correctly identify station ID ***
            match = re.search(r'(\d{3,})', file_name_string)
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
    Main function to orchestrate all tasks.
    """
    # Task 1: Get satellite data averages
    df_satellite = calculate_satellite_averages(SATELLITE_DATA_DIR)
    if df_satellite.empty:
        print("\nHalting script because satellite data processing failed.")
        return

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

    # Task 3: Merge data and calculate yearly correction factors
    print("\n--- Task 3: Merging Data and Calculating Correction Factors ---")
    
    df_merged = pd.merge(df_observed, df_satellite, on=['Station Number', 'Year'], how='inner')

    if df_merged.empty:
        print("\nWARNING: No matching station-year pairs found between observed and satellite data.")
        print("No correction factors will be calculated. Check if station numbers and years overlap between your data sources.")
        return
        
    # Calculate the correction factor
    df_merged['Correction Factor'] = df_merged.apply(
        lambda row: row['Observed Average'] / row['Satellite Average'] if row['Satellite Average'] != 0 else 0,
        axis=1
    )
    
    df_merged.to_csv(YEARLY_CORRECTION_FACTOR_FILE, index=False)
    print(f"Successfully merged data and saved to '{YEARLY_CORRECTION_FACTOR_FILE}'.")
    
    # Task 4: Calculate and save the Grand Correction Factor
    print("\n--- Task 4: Calculating Grand Correction Factor ---")
    
    df_grand_factor = df_merged.groupby('Station Number')['Correction Factor'].mean().reset_index()
    df_grand_factor.rename(columns={'Correction Factor': 'Grand Correction Factor'}, inplace=True)
    
    df_grand_factor.to_csv(GRAND_CORRECTION_FACTOR_FILE, index=False)
    print(f"Successfully calculated and saved grand correction factors to '{GRAND_CORRECTION_FACTOR_FILE}'.")
    
    print("\n\nAll tasks completed successfully!")


if __name__ == '__main__':
    main()
