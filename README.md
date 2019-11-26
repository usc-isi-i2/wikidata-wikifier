# ISI's Wikidata based Wikifier
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
 * Running on http://localhost:7805/ (Press CTRL+C to quit)
```

The service is now running on `http://localhost:7805/wikify`

Example python code to call the wikifier is available at `wikifier/call_wikifier_service.py`

Use curl to call the wikifier,
```
curl -X POST -F 'file=@<path to a csv>' -F columns=<column name to be wikified> http://localhost:7805/wikify -o '<output file path>'
```
