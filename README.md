# ISI's Wikidata based Wikifier

## Input Parameters

The following input parameters are supported,

- `columns`: a comma separated string of column names to be wikified
- `k`: top k results for each cell in the input columns
- `colorized`: {true|false}, if true, returns an excel file with topk results in shaded coloring. Default `false`


## Install Requirements and run the service
```
git clone https://github.com/usc-isi-i2/wikidata-wikifier
cd wikidata-wikifier
python3 -m venv wiki_env
source wiki_env/bin/activate
pip install -r requirements.txt
```

### Update the config

Update the following parameters in the `wikifier/config.json` file, before running the web service ,

- `es_url`: URL of the elasticsearch server. For example: if the ES server is running locally on port 9200, then the url will be http://localhost:9200
- `augmented_dwd_index`: Name of the elasticsearch index
- `model_path`: Absolute path to the neural network model. Relative Path: "./wikifier/model/epoch_17_loss_0.014523069374263287_top1_0.9675043327556326.pth"
- `min_max_scaler_path`: Absolute path to normalizer. Relative Path: "./wikifier/model/tl_pipeline_normalization_factor.pkl"
- `pseudo_gt_model`: Absolute path to neural network for pseudo ground truth. Relative path: "./wikifier/model/epoch_1_loss_0.534353494644165_top1_0.7883487007544007.pth"
- `pseudo_gt_min_max_scaler_path`: Absolute path to pseudo ground truth normalizer. Relative path: "./wikifier/model/normalization_factor.pkl"
- `host`: Web service host, example: "0.0.0.0"
- `port`: Web service port, example: "1703"


### Run the web service

```
python wikifier_service.py
```

You should see output similar to this
```
 * Serving Flask app "wikifier_service" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://0.0.0.0:1703/ (Press CTRL+C to quit)
```

The service is now running on `http://0.0.0.0:1703/wikify`

Example python code to call the wikifier is available at `wikifier/call_wikifier_service.py`

### Use curl to call the wikifier, 

Input file is `wikifier/sample_files/cricketers.csv`, output file: `wikifier/sample_files/cricketers_results.csv` and get 3 results
```
curl -XPOST -F file=@wikifier/sample_files/cricketers.csv \
"https://ckg07.isi.edu/wikifier/wikify?k=3&columns=cricketers" \
-o wikifier/sample_files/cricketers_results.csv
```

### Use curl to call the wikifier, returning colorized excel file
```
curl -XPOST -F file=@wikifier/sample_files/cricketers.csv \
"https://ckg07.isi.edu/wikifier/wikify?k=3&columns=cricketers&colorized=true" \
-o wikifier/sample_files/cricketers_results_colorized.xlsx
```

## Installation via Docker

1. Clone the repository
```
git clone https://github.com/usc-isi-i2/wikidata-wikifier
cd wikidata-wikifier
```

2. Update the following parameters in the `wikifier/config.json`
- `es_url`: URL of the elasticsearch server. For example: if the ES server is running locally on port 9200, then the url will be http://localhost:9200
- `augmented_dwd_index`: Name of the elasticsearch index
- `model_path`: Absolute path to the neural network model. Relative Path: "./wikifier/model/epoch_17_loss_0.014523069374263287_top1_0.9675043327556326.pth"
- `min_max_scaler_path`: Absolute path to normalizer. Relative Path: "./wikifier/model/tl_pipeline_normalization_factor.pkl"
- `pseudo_gt_model`: Absolute path to neural network for pseudo ground truth. Relative path: "./wikifier/model/epoch_1_loss_0.534353494644165_top1_0.7883487007544007.pth"
- `pseudo_gt_min_max_scaler_path`: Absolute path to pseudo ground truth normalizer. Relative path: "./wikifier/model/normalization_factor.pkl"
- `host`: Web service host, example: "0.0.0.0"
- `port`: Web service port, example: "1703"

3. Build the docker image

```
   docker build -t wikidata-wikifier
```

4. Run the docker container using `docker-compose`

```
   docker-compose up -d
```

5. The web service will be running at port `1703`

## Installation of OpenRefine

Instruction from https://docs.openrefine.org/manual/installing

Java#

Java must be installed and configured on your computer to run OpenRefine. The Mac version of OpenRefine includes Java; new in OpenRefine 3.4, there is also a Windows package with Java included.

If you install and start OpenRefine on a Windows computer without Java, it will automatically open up a browser window to the Java downloads page, and you can simply follow the instructions there.

We recommend you download and install Java before proceeding with the OpenRefine installation. Please note that OpenRefine works with Java 14 but not Java 16 or later versions, hopefully this will be fixed in the 3.5 final release (see issue #4106).


Download OpenRefine from openrefine's github (3.4) and edit the main/src/com/google/refine/model/recon/StandardReconConfig.java file.

Edit the batch size to 50 
and scale the timeout 
RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setConnectTimeout(5 * 30 * 1000)
                .setSocketTimeout(5 * 60 * 1000)

## Usage of Reconciliation service using table-linker

After getting OpenRefine and wikidata-wikifier service running, go to http://127.0.0.1:3333/, select a file to reconcile, choose a column, then select reconcile -> start reconciling.

For first time usage, click Add Standard Service and enter the wikidata-service url (an example would be http://localhost:1703/reconcile).

When selecting other columns as contexts, you have to give them a name (can't lave them blank)



