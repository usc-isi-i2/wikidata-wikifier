import pandas as pd
import json
import csv
import multiprocessing
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON

n_processes = 12


class CTA(object):
    def __init__(self, dburi_typeof):
        self.dburi_typeof = dburi_typeof

        self.super_classes = pd.read_csv('./caches/SuperClasses.csv', header=None)[0].tolist()

        self.db_classes = json.load(open('./caches/DBClasses.json'))

        self.db_classes_closure = json.load(open('./caches/DBClassesClosure.json'))
        self.sparqldb = SPARQLWrapper("http://dbpedia.org/sparql")

    def is_instance_of(self, uri):
        self.sparqldb.setQuery(
            "select distinct ?x where {{ <{}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?x . ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .}}".format(
                uri))
        self.sparqldb.setReturnFormat(JSON)
        results = self.sparqldb.query().convert()
        instances = set()
        for result in results["results"]["bindings"]:
            dbp = result['x']['value']
            instances.add(dbp)
        return instances

    def evaluate_class_closure(self, urilist, classuri):
        matches = 0
        classuriclosure = set()
        if classuri in self.db_classes_closure:
            classuriclosure = set(self.db_classes_closure[classuri])
        validuri = []
        for uri in urilist:
            if uri in self.dburi_typeof:
                instances = self.dburi_typeof[uri]
            else:
                instances = self.is_instance_of(uri)
                self.dburi_typeof[uri] = list(instances)

            for instance in instances:
                if instance in classuriclosure:
                    validuri.append(uri)
                    matches += 1
                    break

        score = matches / len(urilist)
        return [score, validuri]

    def find_class(self, urilist, classlist, currentans, ans_list, threshold):
        ans_list.append(currentans)
        if len(classlist) == 0:
            return

        max_score = -1
        max_validuri = []
        max_class = ''
        for superclass in classlist:
            [score, validuri] = self.evaluate_class_closure(urilist, superclass)
            print(score, validuri)
            if max_score < score:
                max_score = score
                max_validuri = validuri
                max_class = superclass

        if max_score >= threshold:
            subclasses = self.db_classes[max_class]
            self.find_class(max_validuri, subclasses, max_class, ans_list, threshold)
            return

    def process(self, idx_group_t_c):
        idx_group, t, c = idx_group_t_c
        index, group = idx_group
        File, Col = index

        urilist = group['answer'].tolist()

        ans_list = []
        self.find_class(urilist, self.super_classes, '', ans_list, t)
        print(ans_list)
        ans_list = ans_list[1:]
        if len(ans_list) <= c:
            [File, Col, ""]
        return [File, Col, " ".join(ans_list)]

    def work_parallel(t, c=1):
        print("min class level" + str(c) + ", running...")
        print("threshold setting:" + str(t) + ", running...")
        work_list = []
        pool = multiprocessing.Pool(processes=n_processes)
        for idx_group in tqdm(targets.groupby(['f', 'c'])):
            work_list.append((idx_group, t, c))

        result_list = pool.map(process, tqdm(work_list))
        with open(output_path + '/CTA_ans_hackv2_' + str(t) + '_' + str(c) + '.csv', 'w', newline='') as myfile:
            for result_row in result_list:
                wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
                wr.writerow(result_row)


from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-e", action="store", type=str, dest="cea_result_path")
parser.add_argument("-o", action="store", type=str, dest="output_path")
parser.add_argument("-c", action="store", type=str, dest="cta_cache_path")
args, _ = parser.parse_known_args()
cea_result_path = args.cea_result_path
output_path = args.output_path
cta_cache_path = args.cta_cache_path

# SuperClassdf = pd.read_csv('{}/SuperClasses.csv'.format(cta_cache_path), header=None)
# super_classes = SuperClassdf[0].tolist()
#
# TypeOfFile = open("{}/TypeOf.json".format(cta_cache_path), "r")
# dburi_typeof = json.loads(TypeOfFile.read())
#
# DBClassesFile = open("{}/DBClasses.json".format(cta_cache_path), "r")
# db_classes = json.loads(DBClassesFile.read())
#
# DBClassesClosureFile = open("{}/DBClassesClosure.json".format(cta_cache_path), "r")
# db_classes_closure = json.loads(DBClassesClosureFile.read())
#
# targets = pd.read_csv(cea_result_path, dtype=object)

work_parallel(0.508, 0)
# t=0.508
# c=0
# a = process(((('la_dataset', 'column'), targets), t, c))
# with open(output_path + '/CTA_ans_hackv2_' + str(t) + '_' + str(c) + '.csv', 'w', newline='') as myfile:
#     for result_row in a:
#         wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
#         wr.writerow(result_row)

open("{}/TypeOf.json".format(cta_cache_path), "w").write(json.dumps(dburi_typeof))
