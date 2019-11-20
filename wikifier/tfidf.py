import math

def compute_tfidf(candidates, feature_count, high_preision_candidates=None):
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
    tfidf_values = [{'tf': 0, 'df': 0, 'idf': 0} for _ in range(feature_count)]
    corpus_num = sum(len(qs) for _, qs in candidates.items())
    
    # compute tf
    for f_idx in range(feature_count):
        for e in candidates:
            for q, v in candidates[e].items():
                if high_preision_candidates:
                    if q == high_preision_candidates.get(e):
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
    return ret


if __name__ == '__main__':
    input_data = {
        'e001': {
            'q1': [0, 0, 0, 0, 1],
            'q2': [0, 1, 0, 1, 0],
            'q3': [0, 0, 1, 0, 0]
        },
        'e002': {
            'q1': [0, 0, 0, 0, 1],
            'q4': [0, 0, 0, 0, 1],
        },
        'e003': {
            'q5': [1, 0, 0, 0, 0],
            'q6': [0, 1, 1, 1, 0],
            'q7': [0, 0, 1, 0, 1]
        },
    }
    print(compute_tfidf(input_data, 5))

    input_data = {
        'e001': {
            'q1': [0, 0, 0, 0, 1] # high precision candidate, remove all the other candidates
        },
        'e002': {
            'q1': [0, 0, 0, 0, 1],
            'q4': [0, 0, 0, 0, 1],
        },
        'e003': {
            'q5': [1, 0, 0, 0, 0],
            'q6': [0, 1, 1, 1, 0],
            'q7': [0, 0, 1, 0, 1]
        },
    }
    print(compute_tfidf(input_data, 5, high_preision_candidates={'e001': 'q1'}))
