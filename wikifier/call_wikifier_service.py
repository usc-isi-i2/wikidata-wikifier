import os
import requests
import pandas as pd
from io import StringIO


def upload_files(file_path, url, column_name, tsv=False, nih=False):
    file_name = os.path.basename(file_path)

    url += f'?k=1&columns={column_name}&colorized=true'
    if tsv:
        url += '&tsv=true'
    if nih:
        url += '&nih=true'
    print(url)

    files = {
        'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
    }
    resp = requests.post(url, files=files)

    s = str(resp.text)
    print('SSS')
    print(s)
    data = StringIO(s)

    df = pd.read_csv(data, header=None)
    df.to_csv(f'sample_files/cricketers_{column_name}.csv', index=False, header=False)
    # print(resp.text)
    return resp.status_code


file_path = 'sample_files/cricketers.csv'

url = "http://localhost:1703/wikify"
# url = "https://dsbox02.isi.edu:8888/wikifier/wikify"
# url = "https://ckg07.isi.edu/wikifier/wikify"
print(upload_files(file_path, url, 'cricketers', tsv=False, nih=False))
