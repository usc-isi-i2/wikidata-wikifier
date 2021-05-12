import os
import requests
import pandas as pd
from io import StringIO


def upload_files(file_path, url, column_name):
    file_name = os.path.basename(file_path)

    url += f'?k=1&columns={column_name}'
    print(url)
    files = {
        'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
    }
    resp = requests.post(url, files=files)

    s = str(resp.content, 'utf-8')
    print('SSS')
    print(s)
    data = StringIO(s)

    df = pd.read_csv(data, header=None)
    df.to_csv('sample_files/cricketers_results.csv'.format(file_name[:-4]), index=False, header=False)
    # print(resp.text)
    return resp.status_code


file_path = '/Users/amandeep/Github/wikidata-wikifier/wikifier/sample_files/cricketers.csv'

url = "http://localhost:1703/wikify"
# url = "https://dsbox02.isi.edu:8888/wikifier/wikify"
print(upload_files(file_path, url, 'cricketers'))
