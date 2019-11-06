from similarity.normalized_levenshtein import NormalizedLevenshtein

label_fields = ['wd_labels', 'wd_aliases', 'person_abbr']


class AddLevenshteinSimilarity(object):
    def __init__(self):
        self.lev = NormalizedLevenshtein()

    def lev_mapper(self, label, wikidata_json):
        qnode = wikidata_json['id']
        max_lev = -1
        max_label = None
        for label_field in label_fields:
            _labels = wikidata_json.get(label_field)
            for l in _labels:
                lev_similarity = self.lev.similarity(label, l)
                if lev_similarity > max_lev:
                    max_lev = lev_similarity
                    max_label = l
        if max_label is not None:
            return (qnode, max_lev, max_label)
        return (None, None, None)

    def lev_mapper_dbpeda(self, label, db_json):
        db_group = db_json['id']
        max_lev = -1
        max_label = None
        anchors = db_json.get('anchor_text', [])
        if not isinstance(anchors, list):
            anchors = [anchors]
        for anchor in anchors:
            lev_similarity = self.lev.similarity(label, anchor)
            if lev_similarity > max_lev:
                max_lev = lev_similarity
                max_label = anchor

        if 'labels' in db_json and 'en' in db_json['labels']:
            en_labels = db_json['labels']['en']
            if not isinstance(en_labels, list):
                en_labels = [en_labels]
                for l in en_labels:
                    lev_similarity = self.lev.similarity(label, l)
                    if lev_similarity > max_lev:
                        max_lev = lev_similarity
                        max_label = l
        if max_label is not None:
            return (db_group, max_lev, max_label)
        return (None, None, None)

    @staticmethod
    def candidates_from_candidate_string(candidate_string):
        qnode_set = set()
        db_group_set = set()
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
                            db_group_set.add(_[0])
                            if len(_) > 1:
                                qnode_set.add(_[1])
                    except:
                        print(c_tuple)

        return list(qnode_set), list(db_group_set)

    def compute_lev(self, label_cand_str, wikidata_index_dict, db_index_dict):
        clean_label = label_cand_str[0]
        candidate_string = label_cand_str[1]
        qnodes, db_groups = self.candidates_from_candidate_string(candidate_string)
        wikidata_jsons = [wikidata_index_dict[qnode] for qnode in qnodes if qnode in wikidata_index_dict]
        db_jsons = [db_index_dict[db_group] for db_group in db_groups]

        results = []
        for wikidata_json in wikidata_jsons:
            r = self.lev_mapper(clean_label, wikidata_json)
            if r[0] is not None:
                results.append('{}:{}'.format(r[0], r[1]))
        for db_json in db_jsons:
            r = self.lev_mapper_dbpeda(clean_label, db_json)
            if r[0] is not None:
                results.append('{}:{}'.format(r[0], r[1]))
        return '@'.join(results)

    def add_lev_feature(self, df, wikidata_index_dict, db_index_dict):
        df['_dummy'] = list(zip(df._clean_label, df._candidates))
        df['lev_feature'] = df['_dummy'].map(lambda x: self.compute_lev(x, wikidata_index_dict, db_index_dict))
        return df
