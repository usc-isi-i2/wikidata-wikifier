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
    colorized_output = request.args.get('colorized', 'false').lower() == 'true'
    nih = request.args.get('nih', 'false').lower() == 'true'
    tsv = request.args.get('tsv', 'fasle').lower() == 'true'

    df = pd.read_csv(request.files['file'], dtype=object) if not tsv else pd.read_csv(request.files['file'],
                                                                                      dtype=object, sep='\t')

    df.fillna('', inplace=True)
    _uuid_hex = uuid4().hex

    _path = 'user_files/{}_{}'.format(columns, _uuid_hex)
    pathlib.Path(_path).mkdir(parents=True, exist_ok=True)

    if nih:
        # this is the NIH case, the output is colorized excel, but we want to return cases where final score
        # is greater that precision_threshold
        output_file = wikifier.wikify(df, columns, output_path=_path, debug=True, k=k,
                                      colorized_output=False,
                                      high_precision=nih)
        odf = pd.read_csv(f'{_path}/{output_file}')
        data = odf.copy()
        _columns = columns.split(",")
        for c in _columns:
            data.loc[data[f'{c}_score'] < 0.9, f'{c}_kg_id'] = 'NIL'
            data.loc[data[f'{c}_score'] < 0.9, f'{c}_score'] = 0

        data.to_csv(f'{_path}/output_high_precision.csv', index=False)
        return send_from_directory(_path, 'output_high_precision.csv')
    else:
        output_file = wikifier.wikify(df, columns, output_path=_path, debug=True, k=k,
                                      colorized_output=colorized_output)
        return send_from_directory(_path, output_file)


if __name__ == '__main__':
    app.run(threaded=True, host=config['host'], port=config['port'])
