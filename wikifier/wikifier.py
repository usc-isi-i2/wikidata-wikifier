import json
import string
import pandas as pd
from tl.candidate_generation.get_exact_matches import ExactMatches
from tl.candidate_generation.get_fuzzy_augmented_matches import FuzzyAugmented
from tl.preprocess.preprocess import canonicalize


class Wikifier(object):

    def __init__(self):
        config = json.load(open('wikifier/config.json'))

        self.asciiiiii = set(string.printable)
        self.es_url = config['es_url']
        self.es_index = config['es_index']
        self.es_search_url = '{}/{}/_search'.format(self.es_url, self.es_index)

    def wikify(self, i_df, columns, format=None, case_sensitive=True):
        if not isinstance(columns, list):
            columns = [columns]

        for column in columns:
            i_df = self.wikify_column(i_df, column, case_sensitive=case_sensitive, debug=True)

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

    def wikify_column(self, df, column, case_sensitive=True, debug=True):
        pass
