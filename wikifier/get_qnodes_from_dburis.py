import json
from argparse import ArgumentParser
from SPARQLWrapper import SPARQLWrapper, JSON


class QNodesFromDBURIs(object):
    def __init__(self):
        config = json.load(open('wikifier/config.json'))
        self.cache_path = 'wikifier/caches/dburi_to_qnode_map.json'
        self.dburi_qnode_map = json.load(open(self.cache_path))
        self.sparql = SPARQLWrapper(config['wd_endpoint'])
        self.sparqldb = SPARQLWrapper(config['db_endpoint'])

    def qnode_from_uri_wiki(self, uris):
        # Method 1 using Wikipedia
        if not uris:
            return []
        ustr = " ".join(["(<{}>)".format(uri) for uri in uris])
        self.sparqldb.setQuery("""
            select ?wikilink ?item where {{VALUES (?item) {{ {} }} ?item foaf:isPrimaryTopicOf ?wikilink }}
            """.format(ustr))
        self.sparqldb.setReturnFormat(JSON)
        try:
            results = self.sparqldb.query().convert()['results']['bindings']
        except:
            results = []
        uri_to_wikilink = {}
        wlinks = []
        for result in results:
            wlink = result['wikilink']['value']
            uri = result['item']['value']
            uri_to_wikilink[uri] = wlink
            wlinks.append(wlink)
        wikistr = ' '.join(["(<{}>)".format(wlink) for wlink in wlinks])
        wikistr = wikistr.replace('http', 'https')
        self.sparql.setQuery("""
            SELECT ?item ?article WHERE {{
                VALUES (?article) {{ {} }} 
                ?article schema:about ?item .
                ?article schema:inLanguage "en" .
            }} 
            """.format(wikistr))
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()['results']['bindings']
        wikilink_to_qnode = {}
        for result in results:
            wlink = result['article']['value'].replace('https', 'http')
            qnode = result['item']['value'].split('/')[4]
            wikilink_to_qnode[wlink] = qnode
        result = []
        for uri in uris:
            if uri in uri_to_wikilink and uri_to_wikilink[uri] in wikilink_to_qnode:
                result.append(wikilink_to_qnode[uri_to_wikilink[uri]])
            else:
                result.append(None)
        return result

    def qnode_from_uri_sameas(self, uris):
        ustr = " ".join(["(<{}>)".format(uri) for uri in uris])
        self.sparqldb.setQuery("""select ?item ?qnode where 
                        {{VALUES (?item) {{ {} }} ?item <http://www.w3.org/2002/07/owl#sameAs> ?qnode .
                        FILTER (SUBSTR(str(?qnode),1, 24) = "http://www.wikidata.org/")
                        }}""".format(ustr))
        self.sparqldb.setReturnFormat(JSON)
        results = self.sparqldb.query().convert()

        uri_to_wiki = {}
        for result in results["results"]["bindings"]:
            uri_to_wiki[result['item']['value']] = result['qnode']['value'].split('/')[-1]

        result = []
        for uri in uris:
            result.append(uri_to_wiki.get(uri, None))
        return result

    def uris_to_qnodes(self, uris):

        remaining_nodes = [uri for uri in uris if uri not in self.dburi_qnode_map]

        while (remaining_nodes):
            try:
                current = remaining_nodes[:50]
                remaining_nodes = remaining_nodes[50:]
                try:
                    result = self.qnode_from_uri_sameas(current)
                except:
                    result = []
                current_remaining = []

                count = len(current)
                for i in range(count):
                    if result and result[i]:
                        self.dburi_qnode_map[current[i]] = result[i]
                    else:
                        current_remaining.append(current[i])

                result = self.qnode_from_uri_wiki(current_remaining)
                count = len(result)
                for i in range(count):
                    self.dburi_qnode_map[current_remaining[i]] = result[i]
            except Exception as e:
                print(e)
                self.write_dburi_qnode_map_to_disk()
                continue

        # self.write_dburi_qnode_map_to_disk()

    def write_dburi_qnode_map_to_disk(self):
        _ = {}
        for k, v in self.dburi_qnode_map.items():
            if v is not None:
                _[k] = v
        open(self.cache_path, 'w').write(json.dumps(_))
