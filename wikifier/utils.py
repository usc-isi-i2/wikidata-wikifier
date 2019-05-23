import sys, os
import warnings
from dsbox.datapreprocessing.cleaner.wikifier import WikifierHyperparams ,Wikifier
from d3m.metadata.base import Metadata, DataMetadata, ALL_ELEMENTS
from d3m.container import List
from d3m.base import utils as d3m_utils
from datamart import upload
from datamart.utilities.utils import SEARCH_URL, PRODUCTION_ES_INDEX, TEST_ES_INDEX
import numpy as np
import logging
import pandas as pd
import copy
import subprocess
from .metadata_wikipedia import metadata_wikipedia
from elasticsearch import Elasticsearch
import config

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)
level = logging.getLevelName('ERROR')
logger.setLevel(level)
Q_NODE_SEMANTIC_TYPE = "http://wikidata.org/qnode"
WIKIDATA_URL = "https://tools.wmflabs.org/sqid/#/view?id="
COLOR_BANK = ['#FFB567', '#36DBFF', '#C1FE9B', '#B89E9E', '#F3FF6D']
ALL_STATES = set(['alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new-hampshire', 'new-jersey', 'new-mexico', 'new-york', 'north-carolina', 'north-dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode-island', 'south-carolina', 'south-dakota', 'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 'west-virginia', 'wisconsin', 'wyoming'])
URL = "http://dsbox02.isi.edu:9200/" # changing this for other queries
INDEX = "datamart_v2"

class DATAMART_ID:
    datamart_id = ""

class Count:
    count = 0

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
# Restore
def enablePrint():
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__

def load_d3m_dataset(input_ds_loc):
    # blockPrint()
    from d3m.container.dataset import D3MDatasetLoader
    loader = D3MDatasetLoader()
    all_dataset_uri = 'file://{}'.format(input_ds_loc)
    input_ds = loader.load(dataset_uri=all_dataset_uri)
    enablePrint()
    return input_ds

def wikifier_for_d3m_all(input_ds):
    # blockPrint()
    wikifier_hyperparams = WikifierHyperparams.defaults()

    wikifier_hyperparams = wikifier_hyperparams.replace({"use_columns":()})
    wikifier_primitive = Wikifier(hyperparams = wikifier_hyperparams)
    output_ds = wikifier_primitive.produce(inputs = input_ds)
    enablePrint()
    return output_ds

def wikifier_for_d3m_fips(input_ds):
    # blockPrint()
    wikifier_hyperparams = WikifierHyperparams.defaults()
    qnodes = List(["P882"])
    wikifier_hyperparams = wikifier_hyperparams.replace({"use_columns":(1,), "specific_q_nodes":qnodes})
    wikifier_primitive = Wikifier(hyperparams = wikifier_hyperparams)
    output_ds = wikifier_primitive.produce(inputs = input_ds)
    enablePrint()
    return output_ds

def print_search_results(search_result):
    out_df = pd.DataFrame()
    for each in search_result:
        out_df = out_df.append(each.display(), ignore_index=True)
    return out_df

def make_clickable_both(val): 
    if '#' not in val:
        return val
    name, url = val.split('#', 1)
    return f'<a href="{url}">{name}</a>'

def highlight_cols0(s):
    color = COLOR_BANK[0]
    return 'background-color: %s' % color

def highlight_cols1(s):
    color = COLOR_BANK[1]
    return 'background-color: %s' % color

def highlight_cols2(s):
    color = COLOR_BANK[2]
    return 'background-color: %s' % color

def highlight_cols3(s):
    color = COLOR_BANK[3]
    return 'background-color: %s' % color

def highlight_cols4(s):
    color = COLOR_BANK[4]
    return 'background-color: %s' % color

def color_ending_with_wikidata(column_names):
    color_column_list = []
    for each in column_names:
        if "_wikidata" in each:
            color_column_list.append(each)
    return color_column_list

def color_after_q_nodes(column_names):
    color_column_list = []
    afterwards_columns = []
    previous_is_wikidata = False
    for each in column_names:
        if previous_is_wikidata and "_wikidata" not in each:
            afterwards_columns.append(each)

        if "_wikidata" in each:
            previous_is_wikidata = True
            color_column_list.append(each)
    return color_column_list, afterwards_columns

def pretty_print(input_ds, ds_type="", display_length=10):
    output_ds = copy.deepcopy(input_ds)
    res_id, inputs_df = d3m_utils.get_tabular_resource(dataset=output_ds, resource_id=None)

    can_mark_dict = {}
    can_mark_from = {}
    can_mark_list = []
    each_column = list(inputs_df.columns)
    if ds_type=="download":
        inputs_df = inputs_df.sort_values(by=['joining_pairs'],ascending=False)
        color_column_list, afterwards_columns = color_after_q_nodes(each_column)
    if ds_type=="wikifier":
        color_column_list = color_ending_with_wikidata(each_column)
    elif ds_type=="wiki_augment":
        color_column_list, afterwards_columns = color_after_q_nodes(each_column)
    else:
        color_column_list = []

    for i in range(inputs_df.shape[1]):
        selector = (res_id,ALL_ELEMENTS, i)
        meta = output_ds.metadata.query(selector)
        if "semantic_types" in meta and Q_NODE_SEMANTIC_TYPE in meta["semantic_types"]:
            can_mark_dict[each_column[i]] = make_clickable_both
            can_mark_list.append(meta["name"])
            temp = meta["name"].split("_wikidata")[0]
            can_mark_from[each_column[i]] = temp
            can_mark_dict[temp] = make_clickable_both
            
    for idx, rows in inputs_df.iterrows():
        for each in can_mark_list:
            if rows[each] is not np.nan:
                rows[can_mark_from[each]] = str(rows[can_mark_from[each]]) + '#'+ WIKIDATA_URL + str(rows[each])
                rows[each] = str(rows[each]) + '#' + WIKIDATA_URL + str(rows[each])
                inputs_df.at[idx,each] = rows[each]
            else:
                inputs_df.at[idx,each] = str(rows[each])

    res = inputs_df.iloc[:display_length,:]
    res = res.style.format(can_mark_dict).applymap(highlight_cols0, subset=pd.IndexSlice[:, color_column_list])
    if ds_type=="wiki_augment" or ds_type=="download":
        res = res.applymap(highlight_cols1, subset=pd.IndexSlice[:, afterwards_columns])
    return res

def download_FBI_data(states=None, python_path=config.python_path, pypath=config.pypath):
    if states is None:
        states = ALL_STATES
    for each_state in states:
        each_state = each_state.lower()
        command_download = python_path + " " + config.pypath +"wikidata-wikifier/wikifier/wikidata/FBI_Crime_Model.py download " + each_state
        p = subprocess.Popen(command_download, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        while p.poll() == None:
            out = p.stdout.readline().strip()
            if out:
                print (bytes.decode(out))

def generate_FBI_data(states=None, python_path=config.python_path, pypath=config.pypath):
    if states is None:
        states = ALL_STATES
    for each_state in states:
        each_state = each_state.lower()
        command_generate = python_path + " " + pypath +"wikidata-wikifier/wikifier/wikidata/FBI_Crime_Model.py generate " + each_state
        p = subprocess.Popen(command_generate, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        while p.poll() == None:
            out = p.stdout.readline().strip()
            if out:
                print (bytes.decode(out))

def upload_FBI_data(states=None, mode=""):
    if states is None:
        states = ALL_STATES

    if mode == "test":
        endpoint = config.endpoint_upload_test
    else:
        endpoint = config.endpoint_upload_main

    for each_state in states:
        each_state = each_state.lower()
        command_add = config.python_path + config.config_upload + endpoint + "--user " + config.user + " --passwd " + config.password + " -f " + each_state + ".ttl"
        command_update_truthy = config.python_path + config.config_update_truthy + endpoint + "--user " + config.user + " --passwd " + config.password + " -f changes_" + each_state + ".tsv"
        
        p = subprocess.Popen(command_add, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        while p.poll() == None:
            out = p.stdout.readline().strip()
            if out:
                print (bytes.decode(out))

        p = subprocess.Popen(command_update_truthy, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)
        while p.poll() == None:
            out = p.stdout.readline().strip()
            if out:
                print (bytes.decode(out))

def clean_FBI_data(mode=""):
    if mode == "test":
        endpoint = config.endpoint_upload_test
    else:
        endpoint = config.endpoint_upload_main
    command_clean = python_path + config_clean_up + endpoint
    subprocess.call(command_clean, stdout=subprocess.PIPE, shell=True, stderr=subprocess.STDOUT)

def upload_wikipedia_data():
    all_metadata = metadata_wikipedia
    succeed = upload(meta_list=[all_metadata], es_index=PRODUCTION_ES_INDEX, deduplicate=False)
    DATAMART_ID.datamart_id = succeed[0]['datamart_id']

def delete_by_IDS(selector, es, index=INDEX):
    for v in selector:
        try:
            es.delete(index=index, doc_type = "_doc", id = v)
        except:
            print("failed to delete", v)

def remove_wikipedia_data(ids):
    if ids is None:
        ids = [DATAMART_ID.datamart_id]
    es = Elasticsearch([URL])
    delete_by_IDS(selector=ids,es = es, index=INDEX)