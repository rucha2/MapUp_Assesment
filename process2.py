#Process 2
import sys
import os
import requests
import argparse
import concurrent.futures
import load_dotenv
# Load environment variables from .env file
load_dotenv.load_dotenv()
print(os.getenv("TOLLGURU_API_KEY"))

def upload_to_tollguru(csv_file, output_dir):
    # API endpoint and parameters
    url = f'{os.getenv("TOLLGURU_API_URL")}/gps-tracks-csv-upload'
    params = {'mapProvider': 'osrm', 'vehicleType': '5AxlesTruck'}

    # Prepare headers with API key
    headers = {'x-api-key': os.getenv("TOLLGURU_API_KEY"), 'Content-Type': 'text/csv'}

    # Read CSV file
    with open(csv_file, 'rb') as file:
        # Send request to TollGuru API
        response = requests.post(url, params=params, data=file, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Construct JSON file name
            json_filename = os.path.join(output_dir, os.path.splitext(os.path.basename(csv_file))[0] + '.json')

            # Save JSON response to file
            with open(json_filename, 'w') as json_file:
                json_file.write(response.text)

            print(f'Successfully processed {csv_file} and saved JSON response to {json_filename}')
        else:
            print(f'Error processing {csv_file}. Status code: {response.status_code}, Message: {response.text}')

def process_gps_files(to_process, output_dir):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    print(to_process, output_dir)
    # List all CSV files in the input directory
    csv_files = [os.path.join(to_process, file) for file in os.listdir(to_process) if file.endswith('.csv')]

    # Use concurrent futures to upload files concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(upload_to_tollguru, csv_files, [output_dir] * len(csv_files))

if __name__ == "__main__":
    # Set up command-line argument parser
    arg1=sys.argv[1]
    arg2=sys.argv[2]
    parser = argparse.ArgumentParser(description='Upload GPS tracks to TollGuru API')
    #args = parser.parse_args()
    process_gps_files(arg1,arg2)
    #process_gps_files(r'C:\Users\admin\Desktop\MapUp-Data-Assessment-E\sample_data\output\process1', r'C:\Users\admin\Desktop\MapUp-Data-Assessment-E\sample_data\output\process4')
    print('done')
