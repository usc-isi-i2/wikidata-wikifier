import json
from SPARQLWrapper import SPARQLWrapper, JSON


class DBURITypeOf:
    def __init__(self):
        config = json.load(open('wikifier/config.json'))
        self.sparql = SPARQLWrapper(config['wd_endpoint'])
        self.sparqldb = SPARQLWrapper(config['db_endpoint'])
        self.super_classes = json.load(open('wikifier/caches/db_class_to_superclass.json'))

    def IsInstanceOf(self, uris):
        uriStr = " ".join(["(<{}>)".format(uri) for uri in uris])
        self.sparqldb.setQuery(
            "select distinct ?x ?y where {{?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?x values(?y) {{ {} }}. ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .}}".format(
                uriStr))
        self.sparqldb.setReturnFormat(JSON)
        results = self.sparqldb.query().convert()
        instances = {}
        for uri in uris:
            instances[uri] = []
        for result in results["results"]["bindings"]:
            uri = result['y']['value']
            _type = result['x']['value']
            if _type in instances[uri]:
                continue
            instances[uri].append(_type)
            if _type in self.super_classes:
                for super_class in self.super_classes[_type]:
                    if super_class not in instances[uri]:
                        instances[uri].append(super_class)
        return instances

    def getRedirects(self, uris):
        uriStr = " ".join(["(<{}>)".format(uri) for uri in uris])
        self.sparqldb.setQuery(
            "select distinct ?x ?y where {{ ?y <http://dbpedia.org/ontology/wikiPageRedirects> ?x  values(?y) {{ {} }}. }}".format(
                uriStr))
        self.sparqldb.setReturnFormat(JSON)
        results = self.sparqldb.query().convert()["results"]["bindings"]
        corrected = {}
        for result in results:
            corrected[result['y']['value']] = result['x']['value']
        return corrected

    def process(self, uris):
        remaining = []
        typeOf = dict()
        count = 0
        while (uris):
            ret = self.IsInstanceOf(uris[:50])
            uris = uris[50:]
            count += 50
            for r in ret:
                if not ret[r]:
                    remaining.append(r)
                else:
                    typeOf[r] = ret[r]

        while (remaining):
            cur = remaining[:50]
            ret = self.getRedirects(cur)
            remaining = remaining[50:]
            count += 50
            reverseRedirect = {}
            for r in cur:
                if r not in ret:
                    typeOf[r] = []
                else:
                    reverseRedirect[ret[r]] = r
            ret = self.IsInstanceOf(list(reverseRedirect.keys()))
            for r in ret:
                original = reverseRedirect[r]
                if not ret[r]:
                    typeOf[original] = []
                else:
                    typeOf[original] = ret[r]

        return typeOf
