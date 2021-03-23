import pathlib
import json
import pandas as pd
from uuid import uuid4
from flask import Flask
from flask import request
from flask import send_from_directory
from wikifier.wikifier import Wikifier

app = Flask(__name__)

wikifier = Wikifier()
config = json.load(open('wikifier/config.json'))


@app.route('/')
def wikidata_wikifier():
    return "Wikidata Wikifier"


@app.route('/wikify', methods=['POST'])
def wikify():
    columns = request.args.get('columns', None)

    k = int(request.args.get('k', 1))

    df = pd.read_csv(request.files['file'], dtype=object)

    df.fillna('', inplace=True)
    _uuid_hex = uuid4().hex

    _path = 'user_files/{}_{}'.format(columns, _uuid_hex)
    pathlib.Path(_path).mkdir(parents=True, exist_ok=True)

    r_df = wikifier.wikify(df, columns, debug=True, k=k)
    r_df.to_csv('{}/results.csv'.format(_path), index=False)
    return send_from_directory(_path, 'results.csv')


if __name__ == '__main__':
    app.run(threaded=True, host=config['host'], port=config['port'])
