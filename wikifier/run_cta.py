import pandas as pd
import json
from SPARQLWrapper import SPARQLWrapper, JSON


class CTA(object):
    def __init__(self, dburi_typeof):
        self.dburi_typeof = dburi_typeof

        self.super_classes = pd.read_csv('./caches/SuperClasses.csv', header=None)[0].tolist()

        self.db_classes = json.load(open('./caches/DBClasses.json'))

        self.db_classes_closure = json.load(open('./caches/DBClassesClosure.json'))
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

    def process(self, df, threshold=0.508, class_level=0):

        urilist = df['answer'].tolist()

        ans_list = []
        self.find_class(urilist, self.super_classes, '', ans_list, threshold)
        ans_list = ans_list[1:]
        if len(ans_list) <= class_level:
            return ""
        return " ".join(ans_list)
