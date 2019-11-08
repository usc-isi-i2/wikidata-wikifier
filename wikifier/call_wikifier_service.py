import os
import requests


def upload_files(file_path, url, column_name):
    file_name = os.path.basename(file_path)
    payload = {
        'columns': column_name
    }
    files = {
        'file': (file_name, open(file_path, mode='rb'), 'application/octet-stream')
    }
    resp = requests.post(url, data=payload, files=files)
    print(resp.text)
    return resp.status_code


file_path = '/Users/amandeep/Github/wikidata-wikifier/wikifier/cricketers.csv'

url = "http://localhost:7805/wikify"
print(upload_files(file_path, url, 'cricketers'))
