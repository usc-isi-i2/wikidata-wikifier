from SPARQLWrapper import SPARQLWrapper, JSON
from argparse import ArgumentParser
import re
import json
import os
import typing

DEFAULT_SAVE_LOCATION = os.path.join(os.getcwd(), "prop_idents_v3.json")


def get_properties(pnode_id):
    """
    Function used to get the corresponding properties of given p node id
    :param pnode_id: a string to indicate a p node
    :return:
    """
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    cur_json = {}
    #sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")
    #sparql.setTimeout(1000)
    sparql.setQuery("""
        SELECT ?x ?v WHERE { ?x wdt:""" + pnode_id + """ ?v. }
    """)
    sparql.setReturnFormat(JSON)
    try:
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            id = result["v"]["value"]
            Q = re.search(r'Q[0-9]+', result["x"]["value"])
            if Q:
                cur_json[id] = Q.group(0)
    except:
        print("failed to find properties on P node" + pnode_id)
    return cur_json


def get_identifiers() -> typing.List[str]:
    """
    The function used to find all P nodes
    :return:
    A list which contains all nodes
    """
    P_nodes = []
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    #sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")
    #sparql.setTimeout(1000)
    sparql.setQuery("""select ?p where {
        ?p wikibase:propertyType wikibase:ExternalId .
        }
        """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    res = results["results"]["bindings"]
    for r in res:
        try:
            P_node = re.search(r'[A-Za-z][0-9]+', r["p"]["value"]).group()
            P_nodes.append(P_node)
            # label = r["P_IDLabel"]["value"]
            # id_label[id]=label
        except:
            print("failed to find P nodes on " + str(r))
    return P_nodes


def save_prop_idents(save_location: str, test_mode: bool=False) -> None:
    """
    main function used to generate the property idents json structure
    :param save_location: path to the location
    :param test_mode: control whether to test on this function or run all
    :return:
    """
    print("The file will be stored at " + save_location)
    id_label = {}  # get_properties("P212")
    P_nodes = get_identifiers()
    prop_idents = {}

    if test_mode:
        print("In test mode, only run first 10 P-nodes")
        P_nodes = P_nodes[:10]

    len_p_nodes = len(P_nodes)

    for idx, P_node in enumerate(P_nodes):
        print("now processing " + P_node + " --> " + str(idx) + "/" + str(len_p_nodes))
        prop_idents[P_node] = get_properties(P_node)
    js = json.dumps(prop_idents)
    f = open("prop_idents_v3.json", "w+")
    f.write(js)
    f.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-l", "--loc", action="store", type=str, dest="save_location", help="the location to save the file, including the file name")
    parser.add_argument("--test", action="store_const", const=True, dest="test_mode", help="whether to enter the test mode (only do process on first 10 p nodes) or not")
    args, _ = parser.parse_known_args()

    if args.save_location:
        save_location = args.save_location
    else:
        save_location = DEFAULT_SAVE_LOCATION

    save_prop_idents(save_location=save_location, test_mode=args.test_mode)

