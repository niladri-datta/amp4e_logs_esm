"""
This script downloads AMP for Endpoints logs and formats the logs for McAfee ESM compatibility
"""
import configparser
import pandas as pd
import requests
import json

# Get runtime values from parameters.ini file
cparser = configparser.ConfigParser()
cparser.read('parameters.ini')
amp_client_id = cparser['AMP4E']['api_client_id']
amp_client_key = cparser['AMP4E']['api_key']
api_endpoint = cparser['AMP4E']['api_endpoint']
url = api_endpoint + 'events'
orig_logfile = cparser['AMP4E']['DefaultFormatLogFile']
esm_logfile = cparser['AMP4E']['ESMFormatLogFile']

try:
    obj_response = requests.get(url, auth=(amp_client_id, amp_client_key))
    dict_json_events = obj_response.json()
    df_events1 = pd.json_normalize(data=dict_json_events, record_path="data", sep="_")

    # Save raw JSON
    raw_json_events = json.dumps(dict_json_events)

    # Create a Series for network_addresses
    ser_addr1 = pd.Series(df_events1['computer_network_addresses'])

    # Extract IP from Series
    list_addr1 = []
    for item in ser_addr1.index:
        list_addr1.append(ser_addr1[item][0]['ip'])

    # Build new Frame from list
    df_addr1 = pd.DataFrame(list_addr1, columns=['computer_ipaddress'])

    # Merge the two Frames
    df_events2 = pd.merge(df_events1,df_addr1,left_index=True, right_index=True)

    # Get rid of group_guids and network_addresses
    df_events2 = df_events2.drop(['group_guids', 'computer_network_addresses'],axis=1)

    # Create flattened JSON
    str_json_events = df_events2.to_json(orient='records', lines=True)

    # Write original JSON logs to file
    with open(orig_logfile, mode='w+') as obj_file2:
        obj_file2.write(raw_json_events)

    # Write ESM compatible JSON logs to file
    with open(esm_logfile, mode='w+') as obj_file1:
        obj_file1.write(str_json_events)

except:
    print('Script encountered an error')
