import pathlib
import pandas as pd
from uuid import uuid4
from flask import Flask
from flask import request
from flask import send_from_directory
from wikifier.wikifier import Wikifier

app = Flask(__name__)

drop_cols = ["_clean_label", "_candidates", "_dummy", "lev_feature", "sorted_lev", "sorted_qnodes", "_dummy_2",
             "top_ranked", "answer", "high_confidence", "sorted_lev_2", "sorted_qnodes_2", "_dummy_3", "answer2",
             "final_confidence", "db_classes", "lev_group"]


@app.route('/')
def wikidata_wikifier():
    return 'Input: CSV and a column name, Output: DBPedia URI for each cell in the given column'


@app.route('/wikify', methods=['POST'])
def wikify():
    df = pd.read_csv(request.files['file'])
    _uuid_hex = uuid4().hex
    _path = 'user_files/{}'.format(_uuid_hex)
    pathlib.Path(_path).mkdir(parents=True, exist_ok=True)
    df.to_csv('{}/input.csv'.format(_path), index=False)
    columns = request.form.get('columns')
    wikifier = Wikifier()
    r_df = wikifier.wikify(df, column=columns)
    r_df.to_csv('{}/results_debug.csv'.format(_path), index=False)
    df = r_df.drop(columns=drop_cols)
    df.to_csv('{}/results.csv'.format(_path), index=False)
    return send_from_directory(_path, 'results.csv')
