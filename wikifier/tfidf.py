import math
import operator


class TFIDF(object):
    def __init__(self, qnodes_dict):
        self.qnodes_dict = qnodes_dict
        self.properties_classes_map = self.create_all_properties_classes_map()

    @staticmethod
    def get_properties_classes_for_qnode(qnode_dict):
        properties_classes_set = set()
        wd_properties = qnode_dict.get('wd_properties', [])
        properties_classes_set.update(wd_properties)

        wd_prop_vals = qnode_dict.get('wd_prop_vals', [])
        for wd_prop_val in wd_prop_vals:
            _ = wd_prop_val.split('#')
            _p = _[0]
            _v = _[1]
            if _p == 'P31':
                properties_classes_set.add(_v)
        return properties_classes_set

    def create_all_properties_classes_map(self):
        properties_classes_set = set()
        for qnode in self.qnodes_dict:
            v = self.qnodes_dict[qnode]
            properties_classes_set.update(self.get_properties_classes_for_qnode(v))
        return {p: idx for idx, p in enumerate(properties_classes_set)}

    def create_feature_vector_dict(self, label_candidates_tuples):
        # creates input for tfidf computation
        feature_vector_dict = {}
        _p_c_len = len(self.properties_classes_map)
        for label_candidates_tuple in label_candidates_tuples:
            label = label_candidates_tuple[0]
            candidates = label_candidates_tuple[1]

            feature_vector_dict[label] = {}
            for candidate in candidates:
                feature_vector = [0] * _p_c_len
                if candidate in self.qnodes_dict:
                    prop_class_list = self.get_properties_classes_for_qnode(self.qnodes_dict[candidate])
                    for _p_c in prop_class_list:
                        if _p_c in self.properties_classes_map:
                            feature_vector[self.properties_classes_map[_p_c]] = 1
                feature_vector_dict[label][candidate] = feature_vector
        return feature_vector_dict

    def compute_tfidf(self, label_candidates_tuples, label_lev_similarity_dict, high_precision_candidates=None, ):
        """
        Compute TF/IDF for all candidates.

        Args:
            candidates:
                ```
                {
                    e1: {
                        q1: [f1, f2, f3],
                        q2: [f1, f2, f3]
                    },
                    'e2': ...
                }
                ```
                `[f1, f2, f3]` is feature vector. All vectors should have same length.
            feature_count: Length of feature vector.
            high_preision_candidates: `{e1: q1, e2: q2}`.
                If None, all qnodes will be used to compute tf.

        Returns:
            ```
            {
                e1: {q1: 1.0, q2: 0.9},
                e2: {q3: 0.1}
            }
        """
        candidates = self.create_feature_vector_dict(label_candidates_tuples)
        feature_count = len(self.properties_classes_map)
        tfidf_values = [{'tf': 0, 'df': 0, 'idf': 0} for _ in range(feature_count)]
        corpus_num = sum(len(qs) for _, qs in candidates.items())

        # compute tf
        for f_idx in range(feature_count):
            for e in candidates:
                for q, v in candidates[e].items():
                    if high_precision_candidates:
                        if q == high_precision_candidates.get(e):
                            if v[f_idx] == 1:
                                tfidf_values[f_idx]['tf'] += 1
                    else:
                        tfidf_values[f_idx]['tf'] += 1

        # compute df
        for f_idx in range(feature_count):
            for e in candidates:
                for q, v in candidates[e].items():
                    if v[f_idx] == 1:
                        tfidf_values[f_idx]['df'] += 1

        # compute idf
        for f_idx in range(len(tfidf_values)):
            if tfidf_values[f_idx]['df'] == 0:
                tfidf_values[f_idx]['idf'] = 0
            else:
                tfidf_values[f_idx]['idf'] = math.log(float(corpus_num) / tfidf_values[f_idx]['df'], 10)

        # compute final score
        ret = {}
        for e in candidates:
            ret[e] = {}
            for q, v in candidates[e].items():
                ret[e][q] = 0
                for f_idx in range(feature_count):
                    ret[e][q] += tfidf_values[f_idx]['tf'] * tfidf_values[f_idx]['idf'] * v[f_idx]

        lev_ret = {}
        for label in ret:
            _dict = ret[label]
            _lev_similarity_dict = label_lev_similarity_dict.get(label, None)

            if _lev_similarity_dict:
                for qnode, tfidf_score in _dict.items():
                    _lev_score = float(_lev_similarity_dict.get(qnode, 0.0))
                    _dict[qnode] = _lev_score * tfidf_score
            lev_ret[label] = _dict

        answer_dict = {}

        for label in lev_ret:
            _dict = lev_ret[label]
            max_v = -1.0
            max_q = None
            for k, v in _dict.items():
                if v > max_v:
                    max_v = v
                    max_q = k
            answer_dict[label] = max_q
        return answer_dict
