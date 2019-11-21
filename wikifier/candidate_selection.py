from operator import itemgetter
import json
import itertools
from similarity.normalized_levenshtein import NormalizedLevenshtein


class CandidateSelection(object):
    def __init__(self, qnode_to_dburi_map, aqs, qnode_typeof_map):
        # self.db_connected_components = db_connected_components
        self.qnode_to_dburi_map = qnode_to_dburi_map
        # self.dburi_to_qnode_map = dburi_to_qnode_map
        # self.dburi_to_labels_dict = dburi_to_labels_dict
        self.aqs = aqs
        self.qnode_typeof_map = qnode_typeof_map
        self.country_dict = json.load(open('wikifier/caches/country_dburi_dict.json'))
        self.states_dict = json.load(open('wikifier/caches/us_states_dburi_dict.json'))
        self.person_classes = json.load(open('wikifier/caches/dbpedia_person_classes.json'))
        self.places_classes = json.load(open('wikifier/caches/dbpedia_place_classes.json'))
        self.normalized_lev = NormalizedLevenshtein()
        db_classes = json.load(open('wikifier/caches/db_classes.json'))
        self.db_classes_parent = self.create_class_parent_hierarchy(db_classes)

    def sort_lev_features(self, lev_feature_s, threshold=0.9):
        qnode_dict = {}
        try:
            qnodes_lev = sorted([z.split(':') for z in lev_feature_s.split('@')], key=itemgetter(1), reverse=True)
            qnodes_lev = list(filter(lambda x: float(x[1]) >= threshold, qnodes_lev))
            for idx in range(len(qnodes_lev)):
                qnode = qnodes_lev[idx][0]
                levscore = qnodes_lev[idx][1]
                qnode_dict[qnode] = levscore
        except:
            return {}

        return qnode_dict

    def sort_lev_features_2(self, lev_feature_s, threshold=0.7):

        r_dict = {}
        try:
            qnodes_lev = sorted([z.split(':') for z in lev_feature_s.split('@')])
            qnodes_lev = list(filter(lambda x: float(x[1]) >= threshold, qnodes_lev))

            for idx in range(len(qnodes_lev)):
                qnode = qnodes_lev[idx][0]
                levscore = qnodes_lev[idx][1]
                r_dict[qnode] = levscore

        except:
            pass

        return r_dict

    def top_ranked(self, df_tuple):
        chosencand = None
        maxscore = 0
        sorted_candidates = df_tuple[0]
        lev_candidates = df_tuple[1]
        candidates_with_one = list()

        for candidate in lev_candidates:
            if lev_candidates[candidate] == '1.0':
                candidates_with_one.append(candidate)

        if len(candidates_with_one) == 1:
            return candidates_with_one[0], 1

        if len(candidates_with_one) > 1:
            return None, 2

        for candidate in sorted_candidates:
            if candidate in lev_candidates:
                score = float(lev_candidates[candidate]) + sorted_candidates[candidate][1]
                if score > maxscore:
                    chosencand = candidate
                    maxscore = score

        return (chosencand, 3) if chosencand is not None else (None, 4)

    def sort_qnodes(self, qnode_string):
        sorted_qnodes = list()

        if qnode_string is not None and isinstance(qnode_string, str):
            q_scores = qnode_string.split('@')
            for q_score in q_scores:
                if q_score != 'nan':
                    q_score_split = q_score.split(':')
                    score = q_score_split[2]
                    id = str(q_score_split[0])
                    qnode = q_score_split[1]

                    if id == '42' or id == '10':
                        sorted_qnodes.append((qnode, float(score) ** 0.25))
                    else:
                        sorted_qnodes.append(
                            (qnode, self.aqs[id]['lambda'] * (float(score) / self.aqs[id]['avg'])))

        results = list()

        for sorted_qnode in sorted_qnodes:
            db_uri = self.qnode_to_dburi_map.get(sorted_qnode[0], None)

            results.append((sorted_qnode[0], db_uri, sorted_qnode[1]))

        results.sort(key=itemgetter(2), reverse=True)
        _dict = {}
        for result in results:
            _dict[result[0]] = [result[1], result[2]]

        return _dict

    def sort_qnodes_2(self, qnode_string):
        result_dict = dict()
        result_dict['wd'] = list()
        result_dict['es'] = list()
        if qnode_string is not None and isinstance(qnode_string, str):
            q_scores = qnode_string.split('@')
            for q_score in q_scores:
                if q_score != 'nan':
                    q_score_split = q_score.split(':')
                    score = q_score_split[2]
                    id = str(q_score_split[0])
                    qnode = q_score_split[1]

                    if id == '42' or id == '10':
                        if id == '42':
                            result_dict['wd'].append((qnode, float(score) ** 0.25))
                        else:
                            result_dict['es'].append((qnode, float(score) ** 0.25))
                    else:
                        result_dict['es'].append(
                            (qnode, self.aqs[id]['lambda'] * (float(score) / self.aqs[id]['avg'])))

        wd_list = result_dict['wd']
        _new_wd_list = list()
        for q_s in wd_list:
            qnode = q_s[0]
            _new_wd_list.append((qnode, float(q_s[1])))

        result_dict['wd'] = sorted(_new_wd_list, key=itemgetter(1), reverse=True)

        es_list = result_dict['es']
        _new_es_list = list()
        for q_s in es_list:
            qnode = q_s[0]
            _new_es_list.append((qnode, float(q_s[1])))

        result_dict['es'] = sorted(_new_es_list, key=itemgetter(1), reverse=True)
        return result_dict

    def lev_similarity(self, label, db_uris, threshold=0.9):
        max_score = -1.0
        best_match = None
        for db_uri in db_uris:
            dburi_part = db_uri.split('/')[-1]
            dburi_part = dburi_part.replace('_', ' ')
            similarity = self.normalized_lev.similarity(label, dburi_part)

            if similarity > max_score:
                max_score = similarity
                best_match = db_uri
        if max_score >= threshold:
            return best_match
        return None

    def choose_candidate_with_cta(self, df_tuple):
        sorted_lev = df_tuple[0]
        sorted_candidates = df_tuple[1]

        cta_class = df_tuple[2]
        answer = df_tuple[3]
        label = df_tuple[4]
        wiki_candidates = sorted_candidates['wd']
        es_candidates = sorted_candidates['es']

        sorted_lev_tuples = list()

        for k, v in sorted_lev.items():
            sorted_lev_tuples.append((k, v))
        if not isinstance(answer, float) and answer is not None and answer.strip() != '' and answer != 'nan':
            # this is the case where the answer was chosen from easy cases
            return answer, 'high_p', 'uniq'

        if cta_class is None or cta_class.strip() == '' or cta_class in self.places_classes:
            # TODO change the states and country dict to have qnodes
            if label in self.states_dict:
                return self.states_dict[label], 'state_dict', 'uniq'
            if label in self.country_dict:
                return self.country_dict[label], 'country_dict', 'uniq'
            lev_cands = sorted(sorted_lev_tuples, key=itemgetter(1), reverse=True)
            lev_cands_groups = itertools.groupby(lev_cands, itemgetter(1))
            for k, v in lev_cands_groups:

                _l_s = [(x[0], x[1]) for x in v]
                _l = [x[0] for x in _l_s]

                lev_sim = self.lev_similarity(label, _l)
                if lev_sim:
                    return lev_sim, 'label_lev_uri_noclass', json.dumps(_l_s)
                for wiki_c in wiki_candidates:
                    if wiki_c[0] in _l:
                        return wiki_c[0], 'wiki_noclass', json.dumps(_l_s)
                for es_c in es_candidates:
                    if es_c[0] in _l:
                        return es_c[0], 'es_noclass', json.dumps(_l_s)

            return None, 'novaliddburi', 'nojoy'

        cta_class_cands = list()

        for qnode, score in sorted_lev.items():
            qnode_class_list = self.qnode_typeof_map.get(qnode)

            if qnode_class_list:
                if cta_class in qnode_class_list:
                    cta_class_cands.append((qnode, score, label))

        if len(cta_class_cands) == 1:
            return cta_class_cands[0][0], 'unambiguous lev', 'uniq'

        if len(cta_class_cands) > 1:
            cta_class_cands = sorted(cta_class_cands, key=itemgetter(1), reverse=True)

            cta_cands_groups = itertools.groupby(cta_class_cands, itemgetter(1))
            for k, v in cta_cands_groups:
                if cta_class in self.person_classes:
                    return None, 'person', 'ambiguous'
                    # _l_s = [(x[0], x[1]) for x in v if x[1] == 1.0]
                else:
                    _l_s = [(x[0], x[1]) for x in v]
                _l = [x[0] for x in _l_s]

                lev_sim = self.lev_similarity(label, _l)

                for wiki_c in wiki_candidates:
                    if wiki_c[0] in _l:
                        return wiki_c[0], 'wiki', json.dumps(_l_s)
                for es_c in es_candidates:
                    if es_c[0] in _l:
                        return es_c[0], 'es', json.dumps(_l_s)
                if lev_sim:
                    return lev_sim, 'label_lev_uri', json.dumps(_l_s)

                # nothing worked, use lev match again

        if len(cta_class_cands) == 0:
            return self.choose_candidate_with_cta(
                (sorted_lev, sorted_candidates, self.db_classes_parent.get(cta_class), answer, label))

        return None, 33, 'empty'

    @staticmethod
    def create_class_parent_hierarchy(db_classes):
        dbclasses_parent = dict()
        for db_class, children in db_classes.items():
            for child in children:
                dbclasses_parent[child] = db_class
        return dbclasses_parent

    def select_high_precision_results(self, df):
        df['sorted_lev'] = df['lev_feature'].map(lambda x: self.sort_lev_features(x))
        df['sorted_qnodes'] = df['_candidates'].map(lambda x: self.sort_qnodes(x))
        df['_dummy_2'] = list(zip(df.sorted_qnodes, df.sorted_lev))
        df['top_ranked'] = df['_dummy_2'].map(lambda x: self.top_ranked(x))
        df['answer'] = df['top_ranked'].map(lambda x: x[0])
        df['high_confidence'] = df['top_ranked'].map(lambda x: x[1])
        return df

    def select_candidates_hard(self, df):
        df['sorted_lev_2'] = df['lev_feature'].map(lambda x: self.sort_lev_features(x, threshold=0.3))
        df['sorted_qnodes_2'] = df['_candidates'].map(lambda x: self.sort_qnodes_2(x))

        df['_dummy_3'] = list(zip(df.sorted_lev_2, df.sorted_qnodes_2, df.cta_class, df.answer, df._clean_label))
        df['answer2'] = df['_dummy_3'].map(lambda x: self.choose_candidate_with_cta(x))
        df['answer_Qnode'] = df['answer2'].map(lambda x: x[0])
        df['answer_dburi'] = df['answer_Qnode'].map(lambda x: self.qnode_to_dburi_map.get(x))
        df['final_confidence'] = df['answer2'].map(lambda x: x[1])
        df['db_classes'] = df['answer_Qnode'].map(lambda x: self.qnode_typeof_map.get(x))
        df['lev_group'] = df['answer2'].map(lambda x: x[2])
        return df
