from flask import Flask
from flask import request
import pandas as pd
from wikifier.wikifier import Wikifier

app = Flask(__name__)


@app.route('/')
def wikidata_wikifier():
    return 'Input: CSV and a column name, Output: DBPedia URI for each cell in the given column'


@app.route('/wikify', methods=['POST'])
def wikify():
    df = pd.read_csv(request.files['file'])
    columns = request.form.get('columns')
    wikifier = Wikifier()
    r_df = wikifier.wikify(df, column=columns)
    r_df.to_csv('results.csv', index=False)
    return 'SUCCESS'
