import os
import requests
import pandas as pd
from io import StringIO


def upload_files(file_path, url, column_name):
    file_name = os.path.basename(file_path)
    payload = {
        'columns': column_name
    }
    files = {
        'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
    }
    resp = requests.post(url, data=payload, files=files)

    s = str(resp.content, 'utf-8')

    data = StringIO(s)

    df = pd.read_csv(data)
    df.to_csv('sample_files/{}_results.csv'.format(file_name[:-4]), index=False)
    return resp.status_code


file_path = 'sample_files/footballers_2.csv'

url = "http://localhost:7805/wikify"
print(upload_files(file_path, url, 'footballers'))
