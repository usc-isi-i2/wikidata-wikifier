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
curl -X POST -F 'file=@wikifier/sample_files/cricketers.csv' http://localhost:1703/wikify?k=3&columns=cricketers \
-o wikifier/sample_files/cricketers_results.csv
```
