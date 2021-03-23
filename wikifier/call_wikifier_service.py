import os
import requests
import pandas as pd
from io import StringIO


def upload_files(file_path, url, column_name):
    file_name = os.path.basename(file_path)
    payload = {
        'columns': column_name,
        'case_sensitive': 'false',
        'k': 5
    }
    files = {
        'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
    }
    resp = requests.post(url, data=payload, files=files)

    s = str(resp.content, 'utf-8')

    data = StringIO(s)

    df = pd.read_csv(data, header=None)
    df.to_csv('sample_files/cricketers_results.csv'.format(file_name[:-4]), index=False, header=False)
    print(resp.text)
    return resp.status_code


file_path = '/Users/amandeep/Github/wikidata-wikifier/wikifier/sample_files/cricketers.csv'

url = "http://localhost:1703/wikify"
print(upload_files(file_path, url, 'cricketers'))
