import os
import json
import pandas as pd
import argparse
import sys
def process_toll_json_files(input_dir, output_dir):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Initialize an empty DataFrame to store the processed data
    processed_data = pd.DataFrame(columns=[
        'unit', 'trip_id', 'toll_loc_id_start', 'toll_loc_id_end',
        'toll_loc_name_start', 'toll_loc_name_end', 'toll_system_type',
        'entry_time', 'exit_time', 'tag_cost', 'cash_cost', 'license_plate_cost'
    ])





    data_frames = []

    # Process each JSON file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            file_path = os.path.join(input_dir, filename)

            # Read JSON file
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)
                print(f'Processing file: {filename}')

                unit = filename.split('_')[0]
                trip_id = filename.split('.')[0]

                start_ids = [toll["start"]["id"] for toll in data["route"]["tolls"]]
                end_ids = [toll["end"]["id"] for toll in data["route"]["tolls"]]
                toll_loc_name_start = [toll["start"]["name"] for toll in data["route"]["tolls"]]
                toll_loc_name_end = [toll["end"]["name"] for toll in data["route"]["tolls"]]
                toll_system_type = [toll["type"] for toll in data["route"]["tolls"]]
                entry_time = [toll["start"]["arrival"]["time"] for toll in data["route"]["tolls"]]
                exit_time = [toll["end"]["arrival"]["time"] for toll in data["route"]["tolls"]]
                tag_cost = [toll["tagCost"] for toll in data["route"]["tolls"]]
                cash_cost = [toll["cashCost"] for toll in data["route"]["tolls"]]
                license_plate_cost = [toll["licensePlateCost"] for toll in data["route"]["tolls"]]

                # Create a DataFrame for the current file's data
                file_data = pd.DataFrame({
                    'unit': [unit] * len(start_ids),
                    'trip_id': [trip_id] * len(start_ids),
                    'toll_loc_id_start': start_ids,
                    'toll_loc_id_end': end_ids,
                    'toll_loc_name_start': toll_loc_name_start,
                    'toll_loc_name_end': toll_loc_name_end,
                    'toll_system_type': toll_system_type,
                    'entry_time': entry_time,
                    'exit_time': exit_time,
                    'tag_cost': tag_cost,
                    'cash_cost': cash_cost,
                    'license_plate_cost': license_plate_cost
                })

                # Append the file data to the list
                data_frames.append(file_data)

    # Concatenate all DataFrames into a single DataFrame
    processed_data = pd.concat(data_frames, ignore_index=True)

    # Save the consolidated data to a CSV file
    output_file_path = os.path.join(output_dir, 'transformed_data.csv')
    processed_data.to_csv(output_file_path, index=False)
    print(f'Consolidated data saved to: {output_file_path}')







                



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Extract trips from GPS data')

    arg1=sys.argv[1]
    arg2=sys.argv[2]

    process_toll_json_files(arg1,arg2)
