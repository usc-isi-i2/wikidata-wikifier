import pathlib
import json
import pandas as pd
from uuid import uuid4
from flask import Flask
from flask import request
from flask import send_from_directory
from wikifier.wikifier import Wikifier
from SPARQLWrapper import SPARQLWrapper, JSON

app = Flask(__name__)

wikifier = Wikifier()
config = json.load(open('wikifier/config.json'))


@app.route('/reconcile', methods=['POST', 'GET'])
def reconcile():
    # deal with callback requests for general info

    query = request.form.get('queries')

    callback = request.args.get('callback', False)

    if query is None:
        if callback:
            content = str(callback) + '(' + str({
                "name": "Table-Linker Reconciliation for OpenRefine (en)",
                "identifierSpace": "http://www.wikidata.org/entity/",
                "schemaSpace": "http://www.wikidata.org/prop/direct/",
                "view": {
                    "url": "https://www.wikidata.org/wiki/{{id}}"
                   }}) + ')'
            return content
        else:
            return {
               "name": "Wikidata Reconciliation for OpenRefine (en)",
               "identifierSpace": "http://www.wikidata.org/entity/",
               "schemaSpace": "http://www.wikidata.org/prop/direct/",
               "view": {
                   "url": "https://www.wikidata.org/wiki/{{id}}"
               }}
    # deal with post/get queries
    else:
        k = 3
        query = json.loads(query)

        df = pd.DataFrame.from_dict(query, orient='index')

        if 'type' in df.columns:
            type = df['type'][0]
        else:
            type = None

        label = []
        for key in query.keys():
            label.append(key)
        df = df.reset_index(drop=True)
        columns = 'query'

        if (len(df)) > 0 and 'properties' in df.columns:
            for ele in (df['properties'][0]):
                df[ele['pid']] = ''
            for i in range(0, len(df)):
                ele = (df['properties'][i])
                for col in ele:
                    df[col['pid']][i] = col['v']
            if 'type' in df.columns:
                df = df.drop('type', 1)
            df = df.drop('properties', 1)

            if 'type_strict' in df.columns:
                df = df.drop('type_strict', 1)

        _uuid_hex = uuid4().hex

        _path = 'user_files/{}_{}'.format(columns, _uuid_hex)
        pathlib.Path(_path).mkdir(parents=True, exist_ok=True)

        wikifier.wikify(df, columns, output_path=_path, debug=True, k=k,
                        colorized_output=True, isa=type)

        df = pd.read_excel(_path + '/colorized.xlsx')

        output = {}
        for ele in label:
            output[ele] = {'result': []}
        for i in range(0, len(df)):

            sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

            sparql.setQuery("""
                SELECT *
                WHERE
                {
                  wd:""" + str(df['top5_class_count'][i]).split(':')[0] + """ rdfs:label ?label .
                  FILTER (langMatches( lang(?label), "EN" ) )
                }
                """)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            results_df = pd.io.json.\
                json_normalize(results['results']['bindings'])

            if len(results_df) == 0:
                output[label[df['row'][i]]]['result'].append({
                    "id": df['kg_id'][i],
                    "name": df['kg_labels'][i],
                    "type": [{"id": str(df['top5_class_count']
                                        [i]).split(':')[0],
                              "name": "None"
                              }],
                    "score": df['siamese_prediction'][i],
                    "match": (float(df['siamese_prediction'][i]) > 0.95 and
                              int(df['rank'][i]) == 1)
                  })

            else:
                output[label[df['row'][i]]]['result'].append({
                    "id": df['kg_id'][i],
                    "name": df['kg_labels'][i],
                    "type": [{"id": str(df['top5_class_count']
                                        [i]).split(':')[0],
                              "name": results_df['label.value'][0]
                              }],
                    "score": df['siamese_prediction'][i],
                    "match": (float(df['siamese_prediction'][i]) > 0.95 and
                              int(df['rank'][i]) == 1)
                  })

        if callback:
            return str(callback) + '(' + str(output) + ')'
        else:
            return json.dumps(output)


@app.route('/')
def wikidata_wikifier():
    return "Wikidata Wikifier"


@app.route('/wikify', methods=['POST'])
def wikify():
    columns = request.args.get('columns', None)

    k = int(request.args.get('k', 1))
    colorized_output = request.args.get('colorized', 'false').lower() == 'true'
    tsv = request.args.get('tsv', 'false').lower() == 'true'

    isa = request.args.get('isa', None)

    sep = '\t' if tsv else ","
    df = pd.read_csv(request.files['file'], dtype=object, sep=sep)
    df.fillna('', inplace=True)

    _uuid_hex = uuid4().hex

    _path = 'user_files/{}_{}'.format(columns, _uuid_hex)
    pathlib.Path(_path).mkdir(parents=True, exist_ok=True)

    output_file = wikifier.wikify(df, columns, output_path=_path,
                                  debug=True, k=k,
                                  colorized_output=colorized_output, isa=isa)
    return send_from_directory(_path, output_file)


if __name__ == '__main__':
    app.run(threaded=True, host=config['host'], port=config['port'])
