import pathlib
import pandas as pd
from uuid import uuid4
from flask import Flask
from flask import request
from flask import send_from_directory
from wikifier.wikifier import Wikifier

app = Flask(__name__)

wikifier = Wikifier()


@app.route('/')
def wikidata_wikifier():
    return "ISI's Wikidata based wikifier"


@app.route('/wikify', methods=['POST'])
def wikify():
    df = pd.read_csv(request.files['file'], dtype=object)
    df.fillna('', inplace=True)
    _uuid_hex = uuid4().hex
    columns = request.form.get('columns')
    format = request.form.get('format')
    _path = 'user_files/{}_{}'.format(columns, _uuid_hex)
    pathlib.Path(_path).mkdir(parents=True, exist_ok=True)
    df.to_csv('{}/input.csv'.format(_path), index=False)

    r_df = wikifier.wikify(df, column=columns, format=format)
    if format == 'wikifier' or format == 'iswc':
        r_df.to_csv('{}/results.csv'.format(_path), index=False, header=False)
    else:
        r_df.to_csv('{}/results.csv'.format(_path), index=False)
    return send_from_directory(_path, 'results.csv')


if __name__ == '__main__':
    app.run(threaded=True, host='localhost', port=7805)
