# ISI's Wikidata based Wikifier

## Input Parameters

The following input parameters are supported,

- `columns`: a comma separated string of column names to be wikified
- `k`: top k results for each cell in the input columns


## Install Requirements and run the service
```
git clone https://github.com/usc-isi-i2/wikidata-wikifier
cd wikidata-wikifier
python3 -m venv wiki_env
source wiki_env/bin/activate
pip install -r requirements.txt

./run_wikifier_service.sh
```
You should see output similar to this
```
 * Serving Flask app "wikifier_service" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://localhost:1703/ (Press CTRL+C to quit)
```

The service is now running on `http://localhost:1703/wikify`

Example python code to call the wikifier is available at `wikifier/call_wikifier_service.py`

Use curl to call the wikifier, input file is `wikifier/sample_files/cricketers.csv`, output file: `wikifier/sample_files/cricketers_results.csv` and get 3 results
```
curl -XPOST -F file=@wikifier/sample_files/cricketers.csv \
"https://ckg07.isi.edu/wikifier/wikify?k=3&columns=cricketers" \
-o wikifier/sample_files/cricketers_results.csv
```

## Installation via Docker

1. Clone the repository
```
git clone https://github.com/usc-isi-i2/wikidata-wikifier
cd wikidata-wikifier
```

2. Update the following parameters in the `wikifier/config.json`
- `es_url`: the elasticsearch server URL
- `augmented_dwd_index`: elasticsearch index name
- `pickled_model_path`: path to the trained model pickle file
- `host`: "0.0.0.0"
- `port`: "1703"

3. Build the docker image

```
   docker build -t wikidata-wikifier
```

4. Run the docker container using `docker-compose`

```
   docker-compose up -d
```

5. The web service will be running at port `1703`

