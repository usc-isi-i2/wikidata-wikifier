import re
import json
import ftfy
import string
import requests
import traceback
from wikifier.run_cta import CTA
from SPARQLWrapper import SPARQLWrapper
from wikifier.dburi_typeof import DBURITypeOf
from wikifier.candidate_selection import CandidateSelection
from wikifier.get_dburis_from_qnodes import DBURIsFromQnodes
from wikifier.get_qnodes_from_dburis import QNodesFromDBURIs
from wikifier.add_levenshtein_similarity_feature import AddLevenshteinSimilarity


class Wikifier(object):

    def __init__(self):
        config = json.load(open('wikifier/config.json'))
        self.asciiiiii = set(string.printable)
        self.es_url = config['es_url']
        self.es_index = config['es_index']
        self.dbpedia_label_index = config['dbpedia_labels_index']

        # average query score
        self.aqs = None
        self.es_doc = config['es_doc']
        self.es_search_url = '{}/{}/{}/_search'.format(self.es_url, self.es_index, self.es_doc)
        self.dbpedia_label_search_url = '{}/{}/{}/_search'.format(self.es_url, self.dbpedia_label_index, self.es_doc)

        self.names_es_index = config['names_es_index']
        self.names_es_doc = config['names_es_doc']
        self.names_es_search_url = '{}/{}/{}/_search'.format(self.es_url, self.names_es_index, self.names_es_doc)

        self.wiki_labels_index = config['wiki_labels_index']
        self.wiki_labels_doc = config['wiki_labels_doc']
        self.wiki_labels_search_url = '{}/{}/{}/_search'.format(self.es_url, self.wiki_labels_index,
                                                                self.wiki_labels_doc)

        self.query_1 = json.load(open('wikifier/queries/wiki_query_1.json'))
        self.query_2 = json.load(open('wikifier/queries/wiki_query_2.json'))
        self.query_more_like_this = json.load(open('wikifier/queries/wiki_query_more_like_this.json'))
        self.query_dbpedia_labels = json.load(open('wikifier/queries/wiki_query_dbpedia_labels.json'))
        self.seen_labels = dict()
        self.db_connected_comps = json.load(open('wikifier/caches/dbpedia_redirect_connected_components.json'))
        self.qnode_dburi_map = json.load(open('wikifier/caches/qnode_to_dburi_map.json'))
        self.dburi_qnode_map = json.load(open('wikifier/caches/dburi_to_qnode_map.json'))
        self.sparql = SPARQLWrapper(config['wd_endpoint'])
        self.sparqldb = SPARQLWrapper(config['db_endpoint'])
        self.qnode_to_labels_dict = {}
        self.dburi_to_labels_dict = {}

    @staticmethod
    def clean_labels(label):
        if not isinstance(label, str):
            return label
        clean_label = ftfy.fix_encoding(label)
        clean_label = ftfy.fix_text(clean_label)
        clean_label = re.sub(r'[[].*[]]', ' ', clean_label)
        clean_label = re.sub(r'[(].*[)]', ' ', clean_label)
        clean_label = clean_label.replace('\\', ' ')
        clean_label = clean_label.replace('/', ' ')
        clean_label = clean_label.replace("\"", ' ')
        clean_label = clean_label.replace("!", ' ')
        clean_label = clean_label.replace("-->", '')
        clean_label = clean_label.replace(">", ' ')
        clean_label = clean_label.replace("<", ' ')
        clean_label = clean_label.replace("-", ' ')
        clean_label = clean_label.replace("^", ' ')
        clean_label = clean_label.replace(":", ' ')
        clean_label = clean_label.replace("+", ' ')
        return clean_label

    @staticmethod
    def create_query(t_query, search_term):
        t_query['query']['bool']['should'][0]['query_string']['query'] = search_term
        t_query['query']['bool']['should'][1]['multi_match']['query'] = search_term
        if len(search_term.split(' ')) == 1:
            t_query['size'] = 100
        else:
            t_query['size'] = 20
        return t_query

    def search_es_names(self, name):
        query = {
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "multi_match": {
                                        "query": name,
                                        "type": "phrase",
                                        "fields": [
                                            "abbr_name"
                                        ],
                                        "slop": 2
                                    }
                                }
                            ]}
                    }
                }
            },
            "size": 50
        }
        response = requests.post(self.names_es_search_url, json=query)
        if response.status_code == 200:
            hits = response.json()['hits']['hits']
            results = [
                '{}:{}:{}'.format('3', x['_id'], x['_score'])
                for x
                in
                hits]
            return results if len(results) > 0 else None
        else:
            print(response.text)
        return None

    def search_es_second(self, search_term):
        search_term_tokens = search_term.split(' ')
        query_type = "phrase"
        slop = 0
        if len(search_term_tokens) <= 3:
            slop = 2

        if len(search_term_tokens) > 3:
            query_type = "phrase"
            slop = 10
        query = self.query_2
        query['query']['function_score']['query']['bool']['must'][0]['multi_match']['query'] = search_term
        query['query']['function_score']['query']['bool']['must'][0]['multi_match']['type'] = query_type
        query['query']['function_score']['query']['bool']['must'][0]['multi_match']['slop'] = slop

        # return the top matched QNode using ES
        response = self.search_es(query, query_id='2')
        if response is not None:
            return response
        elif len(search_term_tokens) > 3:
            for i in range(0, -4, -1):
                t_search_term = ' '.join(search_term_tokens[:i])
                query['query']['function_score']['query']['bool']['must'][0]['multi_match']['query'] = t_search_term
                response = self.search_es(query, query_id='2')
                if response is not None:
                    return response
                else:
                    continue

        return None

    def search_es_more_like_this(self, search_term, query_id='4'):

        search_term_tokens = search_term.split(' ')

        min_term_freq = 1
        tokens_length = len(search_term_tokens)
        max_query_terms = tokens_length

        minimum_should_match = tokens_length - 1

        query = self.query_more_like_this
        query['query']['more_like_this']['like'] = search_term
        query['query']['more_like_this']['min_term_freq'] = min_term_freq
        query['query']['more_like_this']['max_query_terms'] = max_query_terms

        for i in range(minimum_should_match, 0, -1):
            query['query']['more_like_this']['minimum_should_match'] = i
            response = self.search_es(query, query_id=query_id)
            if response is not None:
                return response
            else:
                continue
        return None

    def run_query(self, search_term):
        print(search_term)
        try:
            response = list()

            t_query = self.create_query(self.query_1, search_term)
            response_1 = self.search_es(t_query)

            if response_1 is None:
                t_query = self.create_query(self.query_1, search_term.lower())
                response_1 = self.search_es(t_query)

            if response_1 is None:
                # all has failed lets query on english ascii part of the text
                search_ascii = ''.join(filter(lambda x: x in self.asciiiiii, search_term))
                t_query = self.create_query(self.query_1, search_ascii)
                response_1 = self.search_es(t_query)

            if response_1 is not None:
                response.extend(response_1)

            # Relaxed query
            response_2 = self.search_es_second(search_term)
            if response_2 is not None:
                response.extend(response_2)

            # Query on abbreviated names
            response_3 = self.search_es_names(search_term)
            if response_3 is not None:
                response.extend(response_3)

            response_4 = self.search_es_more_like_this(search_term)
            if response_4 is not None:
                response.extend(response_4)

            t_query = self.create_query(self.query_dbpedia_labels, search_term)
            response_5 = self.search_es_dbpedia(t_query)
            if response_5 is not None:
                response.extend(response_5)

            return '@'.join(response)
        except Exception as e:
            traceback.print_exc()
            raise e

    def search_es(self, query, query_id='1'):
        # return the top matched QNode using ES
        response = requests.post(self.es_search_url, json=query)

        if response.status_code == 200:
            hits = response.json()['hits']['hits']
            results = [
                '{}:{}:{}'.format(query_id, x['_id'], x['_score'])
                for x in hits]
            return results if len(results) > 0 else None
        return None

    def search_es_dbpedia(self, query, query_id='5'):
        # return the top matched QNode using ES
        response = requests.post(self.dbpedia_label_search_url, json=query)
        if response.status_code == 200:
            hits = response.json()['hits']['hits']
            results = ['{}:{}:{}'.format(query_id, self.format_dbpedia_labels_index_results(x), x['_score']) for x in
                       hits]
            for hit in hits:
                id = hit['_id']
                _d = hit["_source"]
                _d['id'] = id
                self.dburi_to_labels_dict[id] = _d
            return results if len(results) > 0 else None
        return None

    @staticmethod
    def format_dbpedia_labels_index_results(doc):
        _source = doc['_source']
        _id = doc['_id']
        if 'qnode' in _source:
            return '{}${}'.format(_id, _source['qnode'])
        return _id

    @staticmethod
    def query_average_scores(df):
        scores = dict()
        lambdas = {
            '1': 1.0,
            '2': 1.0,
            '3': 1.0,
            '4': 1.0,
            '5': 1.0,
            '10': 1,
            '42': 2.0
        }
        qnodes = df['_candidates']
        for qnode_string in qnodes:
            try:
                if qnode_string is not None and isinstance(qnode_string, str):
                    q_scores = qnode_string.split('@')
                    for q_score in q_scores:
                        if q_score != 'nan':
                            q_score = q_score.replace('Category:', '')
                            q_score = q_score.replace('Wikidata:', '')
                            q_score_split = q_score.split(':')
                            score = q_score_split[2]
                            id = str(q_score_split[0])
                            if id not in scores:
                                scores[id] = {
                                    'count': 0,
                                    'cumulative': 0.0,
                                    'lambda': lambdas[id]
                                }
                            scores[id]['count'] += 1
                            scores[id]['cumulative'] += float(score)
            except Exception as e:
                print(e)
                print(qnode_string)
                raise Exception(e)

        for k in scores:
            scores[k]['avg'] = float(scores[k]['cumulative'] / scores[k]['count'])
        return scores

    @staticmethod
    def get_candidates_qnodes_set(df):
        qnode_set = set()
        candidate_strings = df['_candidates'].values
        for candidate_string in candidate_strings:
            if candidate_string is not None and isinstance(candidate_string, str):
                c_tuples = candidate_string.split('@')
                for c_tuple in c_tuples:
                    if c_tuple is not None and isinstance(c_tuple, str) and c_tuple != 'nan':
                        try:
                            vals = c_tuple.split(':')
                            if vals[0] != '5':
                                qnode_set.add(vals[1])
                            else:
                                _ = vals[1].split('$')
                                if len(_) > 1:
                                    qnode_set.add(_[1])
                        except:
                            print(c_tuple)
        return qnode_set

    def get_db_uris(self, df):
        dburis = set()
        candidates_strings = df['_candidates'].values
        for candidates_string in candidates_strings:
            if candidates_string is not None and isinstance(candidates_string, str):
                q_scores = candidates_string.split('@')
                for q_score in q_scores:
                    if q_score != 'nan':
                        q_score_split = q_score.split(':')
                        id = str(q_score_split[0])
                        qnode = q_score_split[1]
                        if id == '5':
                            group_id_tup = qnode.split('$')
                            _group_id = group_id_tup[0]
                            if _group_id in self.db_connected_comps:
                                dburis.update(self.db_connected_comps[_group_id])

        return dburis

    def update_qnode_dburi_caches(self, db_from_q, q_from_db):
        for k, v in db_from_q.qnode_dburi_map.items():
            if v and v not in q_from_db.dburi_qnode_map:
                q_from_db.dburi_qnode_map[v] = k

        for k, v in q_from_db.dburi_qnode_map.items():
            if v and v not in db_from_q.qnode_dburi_map:
                db_from_q.qnode_dburi_map[v] = k

        db_from_q.write_qnode_dburi_map_to_disk()
        q_from_db.write_dburi_qnode_map_to_disk()

    def create_qnode_to_labels_dict(self, qnodes):
        while (len(qnodes) > 0):
            batch = qnodes[:100]
            qnodes = qnodes[100:]
            query = {
                "query": {
                    "ids": {
                        "values": batch
                    }
                },
                "size": len(batch)
            }

            response = requests.post(self.wiki_labels_search_url, json=query)
            if response.status_code == 200:
                es_docs = response.json()['hits']['hits']
                for es_doc in es_docs:
                    self.qnode_to_labels_dict[es_doc['_id']] = es_doc['_source']

    def wikify(self, df, column=None):
        if column is None:
            return df

        if isinstance(column, str):
            # access by column name
            df['_clean_label'] = df[column].map(lambda x: self.clean_labels(x))
        elif isinstance(column, int):
            df['_clean_label'] = df[df.columns[column]].map(lambda x: self.clean_labels(x))

        # find the candidates
        df['_candidates'] = df['_clean_label'].map(lambda x: self.run_query(x))
        df.to_csv('candidates.csv', index=False)
        self.aqs = self.query_average_scores(df)
        all_qnodes = self.get_candidates_qnodes_set(df)
        all_dburis = self.get_db_uris(df)
        db_from_q = DBURIsFromQnodes()
        q_from_db = QNodesFromDBURIs()
        q_from_db.uris_to_qnodes(list(all_qnodes))
        db_from_q.get_dburis_from_qnodes(list(all_dburis))

        for _dburi in list(all_dburis):
            if _dburi in q_from_db.dburi_qnode_map:
                _qnode = q_from_db.dburi_qnode_map[_dburi]
                if _qnode is not None:
                    all_qnodes.add(_qnode)

        for qnode in all_qnodes:
            _dburi = db_from_q.qnode_dburi_map.get(qnode, None)
            if _dburi:
                all_dburis.add(_dburi)

        dbto = DBURITypeOf()

        dburi_typeof_map = dbto.process(list(all_dburis))
        cta = CTA(dburi_typeof_map)

        self.create_qnode_to_labels_dict(list(all_qnodes))

        lev_similarity = AddLevenshteinSimilarity()
        df = lev_similarity.add_lev_feature(df, self.qnode_to_labels_dict, self.dburi_to_labels_dict)

        cs = CandidateSelection(self.db_connected_comps, self.qnode_dburi_map, self.dburi_qnode_map,
                                self.dburi_to_labels_dict, self.aqs, dburi_typeof_map)
        df = cs.select_high_precision_results(df)
        df_high_precision = df.loc[df['answer'].notnull()]
        cta_class = cta.process(df_high_precision)

        df['cta_class'] = cta_class
        df = cs.select_candidates_hard(df)

        self.update_qnode_dburi_caches(db_from_q, q_from_db)
        return df
