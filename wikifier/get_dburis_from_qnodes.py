import re
import json
from SPARQLWrapper import SPARQLWrapper, JSON


class DBURIsFromQnodes(object):
    def __init__(self):
        config = json.load(open('wikifier/config.json'))
        self.cache_path = 'wikifier/caches/qnode_to_dburi_map.json'
        self.qnode_dburi_map = json.load(open(self.cache_path))
        self.sparql = SPARQLWrapper(config['wd_endpoint'])
        self.sparqldb = SPARQLWrapper(config['db_endpoint'])

    def get_dburi_from_wikipedia(self, qnodes):
        '''
        This is approach1: We go from QNode to DBPedia using Wikipedia
        :param QNodes:
        :return: DBpedia URIs for each QNode
        '''
        if not qnodes:
            return []
        QNodeWikipediaMap = {}
        QStr = " ".join(["(wd:{})".format(qnode) for qnode in qnodes])
        self.sparql.setQuery("""
        SELECT ?item ?article WHERE {{
            OPTIONAL {{
            ?article schema:about ?item VALUES (?item) {{ {} }} .
            ?article schema:inLanguage "en" .
            FILTER (SUBSTR(str(?article), 1, 25) = "https://en.wikipedia.org/")
            }}
        }} 
        """.format(QStr))
        self.sparql.setReturnFormat(JSON)
        try:
            results = self.sparql.query().convert()['results']['bindings']

        except:
            results = []
        for result in results:
            if not result:
                continue
            temp = result['article']['value'].split('/')[-1]
            temp = re.sub(r'([()!])', r'\\\1', temp)
            temp = temp.replace(',', '')
            temp = temp.replace('.', '')
            temp = temp.replace(' ', '_')
            QNodeWikipediaMap[result['item']['value'].split('/')[-1]] = temp

        wikiLabels = list(QNodeWikipediaMap.values())
        wikiStr = " ".join(["(wikipedia-en:{})".format(wikiLabel) for wikiLabel in wikiLabels])

        self.sparqldb.setQuery("""
        select ?dbpedia ?item where {{?dbpedia foaf:isPrimaryTopicOf ?item VALUES (?item) {{ {} }} }}
        """.format(wikiStr))
        self.sparqldb.setReturnFormat(JSON)
        try:
            results = self.sparqldb.query().convert()['results']['bindings']
        except:
            results = []
        wikipediaDBPediaMap = {}
        for result in results:
            wikipediaDBPediaMap[result['item']['value'].split('/')[-1]] = result['dbpedia']['value']

        result = []
        for qnode in qnodes:
            if qnode in QNodeWikipediaMap and QNodeWikipediaMap[qnode] in wikipediaDBPediaMap:
                result.append(wikipediaDBPediaMap[QNodeWikipediaMap[qnode]])
            else:
                result.append(None)
        return result

    def update_dburis(self, qnodes):
        total = 0
        while (qnodes):
            try:
                current = qnodes[:50]
                qnodes = qnodes[50:]

                result = self.get_dburi_from_wikipedia(current)
                count = len(result)
                for i in range(count):
                    self.qnode_dburi_map[current[i]] = result[i]
                total += 50

                if total % 1000 == 0:
                    self.write_qnode_dburi_map_to_disk()
            except:
                continue

        self.write_qnode_dburi_map_to_disk()

    def write_qnode_dburi_map_to_disk(self):
        _ = {}
        for k, v in self.qnode_dburi_map.items():
            if v is not None:
                _[k] = v
        open(self.cache_path, 'w').write(json.dumps(_))

    def get_dburis_from_qnodes(self, qnodes):
        # qnodes_new = [qnode for qnode in qnodes if qnode not in self.qnode_dburi_map or (
        #         qnode in self.qnode_dburi_map and self.qnode_dburi_map[qnode] is None)]
        qnodes_new = [qnode for qnode in qnodes if qnode not in self.qnode_dburi_map]
        print('New Qnodes to get DB URIs for: {}'.format(len(qnodes_new)))
        if qnodes_new:
            self.update_dburis(qnodes_new)
