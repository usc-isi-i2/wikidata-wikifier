import pandas as pd
import json
from SPARQLWrapper import SPARQLWrapper, JSON


class CTA(object):
    def __init__(self, qnode_typeof):
        self.qnode_typeof = qnode_typeof

        # self.super_classes = pd.read_csv('wikifier/caches/SuperClasses.csv', header=None)[0].tolist()
        self.super_classes = ['Q35120'] #Entity class at root of hierarchy

        self.db_classes = json.load(open('wikifier/caches/DBClasses.json'))

        # self.db_classes_closure = json.load(open('wikifier/caches/DBClassesClosure.json'))
        self.wikidata_classes_closure = json.load(open('wikifier/caches/wikidata_class_closure.json'))
        self.sparqldb = SPARQLWrapper("http://dbpedia.org/sparql")

    def is_instance_of(self, uri):
        self.sparqldb.setQuery(
            "select distinct ?x where {{ <{}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?x . "
            "?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .}}".format(
                uri))
        self.sparqldb.setReturnFormat(JSON)
        results = self.sparqldb.query().convert()
        instances = set()
        for result in results["results"]["bindings"]:
            dbp = result['x']['value']
            instances.add(dbp)
        return instances

    def evaluate_class_closure(self, qnodes, classuri):
        matches = 0
        classuriclosure = set()
        if classuri in self.wikidata_classes_closure:
            classuriclosure = set(self.wikidata_classes_closure[classuri])
        valid_qnode = []
        for qnode in qnodes:
            if qnode in self.qnode_typeof:
                instances = self.qnode_typeof[qnode]
            else:
                instances = self.is_instance_of(qnode)
                self.qnode_typeof[qnode] = list(instances)

            for instance in instances:
                if instance in classuriclosure:
                    valid_qnode.append(qnode)
                    matches += 1
                    break

        score = matches / len(qnodes)
        return [score, valid_qnode]

    def find_class(self, qnodes, classlist, currentans, ans_list, threshold):
        ans_list.append(currentans)
        if len(classlist) == 0:
            return

        max_score = -1
        max_validuri = []
        max_class = ''
        for superclass in classlist:
            [score, validuri] = self.evaluate_class_closure(qnodes, superclass)
            if max_score < score:
                max_score = score
                max_validuri = validuri
                max_class = superclass

        if max_score >= threshold:
            subclasses = self.db_classes[max_class]
            self.find_class(max_validuri, subclasses, max_class, ans_list, threshold)
            return

    def process(self, df, threshold=0.508, class_level=0):

        qnodes = df['answer'].tolist()
        if len(qnodes) == 0:
            return ""
        ans_list = []
        self.find_class(qnodes, self.super_classes, '', ans_list, threshold)
        ans_list = ans_list[1:]
        if len(ans_list) <= class_level:
            return ""
        return " ".join(ans_list)

    def process_frequency_match(self, qnodes):
        if len(qnodes) == 0:
            return ""

        class_list = list()
        for qnode in qnodes:
            _list = [x for x in self.dburi_typeof.get(qnode, []) if x.startswith('http://dbpedia.org')]
            class_list.extend(_list)
        wiki_class = pd.Series(list(class_list), name='Class')
        value_counts = wiki_class.value_counts(normalize=True, dropna=False)
        frequency = pd.DataFrame(value_counts.rename_axis('Class').reset_index(name='Frequency'))
        return frequency['Class'].tolist()[0]
