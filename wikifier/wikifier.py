import re
import json
import ftfy
import string
import requests
import traceback
import pandas as pd
from wikifier.tfidf import TFIDF
from wikifier.run_cta import CTA
from wikifier.candidate_selection import CandidateSelection
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

        self.wikidata_dbpedia_joined_index = config['wikidata_dbpedia_joined_index']
        self.wikidata_dbpedia_joined_doc = config['wikidata_dbpedia_joined_doc']
        self.wiki_dbpedia_joined_search_url = '{}/{}/{}/_search'.format(self.es_url, self.wikidata_dbpedia_joined_index,
                                                                        self.wikidata_dbpedia_joined_doc)

        self.query_1 = json.load(open('wikifier/queries/wiki_query_1.json'))
        self.query_2 = json.load(open('wikifier/queries/wiki_query_2.json'))
        self.query_more_like_this = json.load(open('wikifier/queries/wiki_query_more_like_this.json'))
        self.query_dbpedia_labels = json.load(open('wikifier/queries/wiki_query_dbpedia_labels.json'))
        self.seen_labels = dict()

        self.lev_similarity = AddLevenshteinSimilarity()

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

    def search_es_exact_match(self, search_term, query_id='6'):
        query = {
            "query": {
                "term": {
                    "all_labels.keyword": {
                        "value": search_term
                    }
                }
            },
            "size": 100
        }

        query_lower = {
            "query": {
                "term": {
                    "all_labels.keyword_lower": {
                        "value": search_term.lower()
                    }
                }
            },
            "size": 100
        }
        response = self.search_es(query, query_id=query_id, es_url=self.wiki_dbpedia_joined_search_url)
        if response is not None:
            return response
        else:
            response = self.search_es(query_lower, query_id=query_id, es_url=self.wiki_dbpedia_joined_search_url)
            if response is not None:
                return response
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

            # t_query = self.create_query(self.query_dbpedia_labels, search_term)
            # response_5 = self.search_es_dbpedia(t_query)
            # if response_5 is not None:
            #     response.extend(response_5)

            response_6 = self.search_es_exact_match(search_term)
            if response_6 is not None:
                response.extend(response_6)

            return '@'.join([r for r in response if r.strip() != ''])
        except Exception as e:
            traceback.print_exc()
            raise e

    def search_es(self, query, query_id='1', es_url=None):
        if es_url is None:
            es_url = self.es_search_url

        # return the top matched QNode using ES
        response = requests.post(es_url, json=query)

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
            '6': 1.0,
            '10': 1,
            '42': 2.0
        }
        qnodes = df['_candidates']
        for qnode_string in qnodes:
            try:
                if qnode_string is not None and isinstance(qnode_string, str) and qnode_string.strip() != '':
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

    def get_candidates_qnodes_set(self, df):
        qnode_set = set()
        candidate_strings = df['_candidates'].values
        for candidate_string in candidate_strings:
            qnode_set.update(self.create_list_from_candidate_string(candidate_string))
        return qnode_set

    @staticmethod
    def create_list_from_candidate_string(candidate_string):
        qnode_set = set()
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
        return list(qnode_set)

    def create_qnode_to_labels_dict(self, qnodes):
        qnode_to_labels_dict = dict()
        if not isinstance(qnodes, list):
            qnodes = [qnodes]

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

            response = requests.post(self.wiki_dbpedia_joined_search_url, json=query)
            if response.status_code == 200:
                es_docs = response.json()['hits']['hits']
                for es_doc in es_docs:
                    qnode_to_labels_dict[es_doc['_id']] = es_doc['_source']
        return qnode_to_labels_dict

    @staticmethod
    def create_qnode_to_type_dict(qnode_to_labels_dict):
        qnode_to_type_map = dict()
        for qnode in qnode_to_labels_dict:
            dbpedia_instance_types = qnode_to_labels_dict[qnode]['db_instance_types']
            qnode_to_type_map[qnode] = list(set(dbpedia_instance_types))
        return qnode_to_type_map

    @staticmethod
    def create_qnode_to_dburi_map(qnode_to_labels_dict):
        qnode_to_dburi_map = {}
        for qnode in qnode_to_labels_dict:
            dbpedia_urls = qnode_to_labels_dict[qnode]['dbpedia_urls']
            if len(dbpedia_urls) > 0:
                for dbpedia_url in dbpedia_urls:
                    if dbpedia_url.startswith('http://dbpedia.org/resource'):
                        # english dbpedia
                        qnode_to_dburi_map[qnode] = dbpedia_url
                if qnode not in qnode_to_dburi_map:
                    # no english dbpedia url:
                    qnode_to_dburi_map[qnode] = dbpedia_urls[0]  # just pick the first one
            else:
                qnode_to_dburi_map[qnode] = None
        return qnode_to_dburi_map

    @staticmethod
    def create_high_precision_tfidf_input(label_hp_candidate_tuples):
        _ = {}
        for label_hp_candidate_tuple in label_hp_candidate_tuples:
            label = label_hp_candidate_tuple[0]
            candidate = label_hp_candidate_tuple[1]
            _[label] = candidate
        return _

    def get_dburi_for_qnode(self, qnode, qnode_dburi_map):
        if qnode is None:
            return None
        if qnode_dburi_map.get(qnode) is not None:
            return qnode_dburi_map[qnode]

        _qdict = self.create_qnode_to_labels_dict(qnode)
        return self.create_qnode_to_dburi_map(_qdict)[qnode]

    @staticmethod
    def create_lev_similarity_dict(label_lev_tuples, candidate_selection_object):
        if not label_lev_tuples:
            return {}
        label_lev_similarity_dict = {}
        for label_lev_tuple in label_lev_tuples:
            if label_lev_tuple:
                label = label_lev_tuple[0]
                lv_qnodes_string = label_lev_tuple[1]
                _l_dict = candidate_selection_object.sort_lev_features(lv_qnodes_string, threshold=0.0)
                label_lev_similarity_dict[label] = _l_dict
        return label_lev_similarity_dict

    @staticmethod
    def create_answer_dict(df):
        answers = list(zip(df.label, df.cta_class, df.answer_Qnode, df.answer_dburi))
        _dict = {}
        for answer in answers:
            _dict[answer[0]] = (answer[1], answer[2], answer[3])
        return _dict

    def wikify_column(self, i_df, column, case_sensitive=True):
        raw_labels = list()
        if isinstance(column, str):
            # access by column name
            raw_labels = list(i_df[column].unique())
        elif isinstance(column, int):
            raw_labels = list(i_df.iloc[:, column].unique())

        _new_i_list = []
        for label in raw_labels:
            _new_i_list.append({'label': label, '_clean_label': self.clean_labels(label)})

        df = pd.DataFrame(_new_i_list)

        # find the candidates
        df['_candidates'] = df['_clean_label'].map(lambda x: self.run_query(x))
        df['_candidates_list'] = df['_candidates'].map(lambda x: self.create_list_from_candidate_string(x))

        self.aqs = self.query_average_scores(df)
        all_qnodes = self.get_candidates_qnodes_set(df)

        qnode_to_labels_dict = self.create_qnode_to_labels_dict(list(all_qnodes))
        qnode_dburi_map = self.create_qnode_to_dburi_map(qnode_to_labels_dict)
        qnode_typeof_map = self.create_qnode_to_type_dict(qnode_to_labels_dict)
        cta = CTA(qnode_typeof_map)
        tfidf = TFIDF(qnode_to_labels_dict)

        df = self.lev_similarity.add_lev_feature(df, qnode_to_labels_dict, case_sensitive)

        cs = CandidateSelection(qnode_dburi_map, self.aqs, qnode_typeof_map)
        df = cs.select_high_precision_results(df)

        df_high_precision = df.loc[df['answer'].notnull()]
        label_lev_similarity_dict = self.create_lev_similarity_dict(list(zip(df._clean_label, df.lev_feature)), cs)

        label_hp_candidate_tuples = list(zip(df_high_precision._clean_label, df_high_precision.answer))
        high_precision_candidates = self.create_high_precision_tfidf_input(label_hp_candidate_tuples)
        label_candidates_tuples = list(zip(df._clean_label, df._candidates_list))
        tfidf_answer = tfidf.compute_tfidf(label_candidates_tuples, label_lev_similarity_dict,
                                           high_precision_candidates=high_precision_candidates)

        cta_class = cta.process(df_high_precision)

        df['cta_class'] = cta_class.split(' ')[-1]
        df['answer_Qnode'] = df['_clean_label'].map(lambda x: tfidf_answer.get(x))
        df['answer_dburi'] = df['answer_Qnode'].map(lambda x: self.get_dburi_for_qnode(x, qnode_dburi_map))
        answer_dict = self.create_answer_dict(df)

        i_df['{}_cta_class'.format(column)] = i_df[column].map(lambda x: answer_dict[x][0])
        i_df['{}_answer_Qnode'.format(column)] = i_df[column].map(lambda x: answer_dict[x][1])
        i_df['{}_answer_dburi'.format(column)] = i_df[column].map(lambda x: answer_dict[x][2])
        return i_df

    def wikify(self, i_df, columns, format=None, case_sensitive=True):
        if not isinstance(columns, list):
            columns = [columns]

        for column in columns:
            i_df = self.wikify_column(i_df, column, case_sensitive=case_sensitive)

        if format and format.lower() == 'iswc':
            _o = list()
            for index, row in i_df.iterrows():
                for column in columns:
                    _o.append({'column': column, 'r': index, 'q': row['{}_answer_Qnode'.format(column)]})
            return pd.DataFrame(data=_o)

        if format and format.lower() == 'wikifier':
            _o = list()
            for index, row in i_df.iterrows():
                for column in columns:
                    _o.append({'f': '', 'c': '', 'l': row[column], 'q': row['{}_answer_Qnode'.format(column)]})
            return pd.DataFrame(data=_o)

        return i_df
