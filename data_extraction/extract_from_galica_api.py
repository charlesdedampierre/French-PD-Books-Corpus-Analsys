import requests
import xml.etree.ElementTree as ET
import multiprocessing
from tqdm import tqdm
import json

# Function to fetch and parse XML for a single ARK value
def fetch_and_parse_xml(ark):
    url = f'https://gallica.bnf.fr/services/OAIRecord?ark={ark}'
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Parse the XML response
            xml_data = response.text
            root = ET.fromstring(xml_data)
            
            # Create a dictionary to store the extracted data
            data = {}
            
            # Extract and populate the dictionary with relevant data
            for elem in root.findall('.//dc:*', namespaces={'dc': 'http://purl.org/dc/elements/1.1/'}):
                tag = elem.tag.split('}')[-1]
                text = elem.text
                data[tag] = text
            
            # Extract and add the specified setSpec elements
            set_specs = [elem.text for elem in root.findall('.//setSpec')]
            data['setSpec'] = set_specs
            data['file_id'] = ark
            
            return data
        else:
            print(f'Error: Request failed with status code {response.status_code}')
    except Exception as e:
        print(f'Error: {str(e)}')

# Function to process multiple ARK values using multiprocessing
def process_multiple_ark_values(ark_values):
    extracted_data = []
    for ark_value in tqdm(ark_values):
        try:
            result = fetch_and_parse_xml(ark_value)
        except:
            result = None
        
        extracted_data.append(result)
    return extracted_data

if __name__ == "__main__":
    
    import pandas as pd
    data = pd.read_csv('../data/raw_data/galica_mon_sampling_title.csv', index_col = [0])
    list_ids = list(data['file_id'])
    
    num_processes = multiprocessing.cpu_count()-2  # Use the number of available CPU cores
    pool = multiprocessing.Pool(processes=num_processes)
    
    # Use the pool to process the ARK values in parallel
    results = list(tqdm(pool.imap(fetch_and_parse_xml, list_ids), total=len(list_ids)))
    
    # Close the pool
    pool.close()
    pool.join()
    # Now, extracted_data contains the results for all ARK values processed in parallel

    # Save the results as a JSONL file
    with open('../data/raw_data/results_galica_api_sample.jsonl', 'w') as jsonl_file:
        for result in results:
            if result is not None:
                jsonl_file.write(json.dumps(result) + '\n')
