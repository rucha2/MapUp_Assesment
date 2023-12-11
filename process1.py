import pandas as pd
import os
import argparse
import sys

def process_gps_data(parquet_file, output_dir):
    # Read Parquet file into a DataFrame
    df = pd.read_parquet(parquet_file,engine='pyarrow')
    #df['timestamp']= pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%SZ')

    
    # Sort the DataFrame by unit and timestamp
    df.sort_values(by=['unit', 'timestamp'], inplace=True)

    # Create a new column 'trip_number' to identify trips
    df['trip_number'] = (df.groupby('unit')['timestamp']
                         .diff()
                         .gt(pd.Timedelta(hours=7))
                         .cumsum()
                         .fillna(0)
                         .astype(int))

    # Iterate over unique units and write trip-specific CSV files
    for unit, unit_df in df.groupby('unit'):
        for trip_number, trip_df in unit_df.groupby('trip_number'):
            # Construct CSV file name
            csv_filename = os.path.join(output_dir, f'{unit}_{trip_number}.csv')

            # Write trip-specific CSV file
            trip_df[['latitude', 'longitude', 'timestamp']].to_csv(csv_filename,index=False, date_format='%Y-%m-%dT%H:%M:%SZ')
    print('Done')

if __name__ == "__main__":
    # Set up command-line argument parser
    parser = argparse.ArgumentParser(description='Extract trips from GPS data')
    #parser.add_argument('--to_process', required=True, help='Path to the Parquet file to be processed.')
    #parser.add_argument('--output_dir', required=True, help='Folder to store the resulting CSV files.')
    #args = parser.parse_args()
    arg1=sys.argv[1]
    arg2=sys.argv[2]
    # Process GPS data and extract trips
    process_gps_data(arg1,arg2)
