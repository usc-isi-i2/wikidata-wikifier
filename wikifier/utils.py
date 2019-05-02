import sys, os
import warnings
from dsbox.datapreprocessing.cleaner.wikifier import WikifierHyperparams ,Wikifier
from d3m.container import List
warnings.filterwarnings("ignore")
import logging
logger = logging.getLogger(__name__)
level = logging.getLevelName('ERROR')
logger.setLevel(level)
# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

def load_d3m_dataset(input_ds_loc):
    blockPrint()
    from d3m.container.dataset import D3MDatasetLoader
    loader = D3MDatasetLoader()
    all_dataset_uri = 'file://{}'.format(input_ds_loc)
    input_ds = loader.load(dataset_uri=all_dataset_uri)
    enablePrint()
    return input_ds

def wikifier_for_d3m_all(input_ds):
    blockPrint()
    wikifier_hyperparams = WikifierHyperparams.defaults()

    wikifier_hyperparams = wikifier_hyperparams.replace({"use_columns":()})
    wikifier_primitive = Wikifier(hyperparams = wikifier_hyperparams)
    output_ds = wikifier_primitive.produce(inputs = input_ds)
    enablePrint()
    return output_ds

def wikifier_for_d3m_fips(input_ds):
    blockPrint()
    wikifier_hyperparams = WikifierHyperparams.defaults()
    qnodes = List(["P882"])
    wikifier_hyperparams = wikifier_hyperparams.replace({"use_columns":(1,), "specific_q_nodes":qnodes})
    wikifier_primitive = Wikifier(hyperparams = wikifier_hyperparams)
    output_ds = wikifier_primitive.produce(inputs = input_ds)
    enablePrint()
    return output_ds