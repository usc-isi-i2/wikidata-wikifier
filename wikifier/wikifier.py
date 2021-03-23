import json
import string
import pandas as pd
from tl.candidate_generation.get_exact_matches import ExactMatches
from tl.candidate_generation.get_fuzzy_augmented_matches import FuzzyAugmented
from tl.preprocess.preprocess import canonicalize, clean
from tl.features.smallest_qnode_number import smallest_qnode_number
from tl.features.string_similarity import StringSimilarity
from tl.features.feature_voting import feature_voting
from tl.features.external_embedding import EmbeddingVector
from tl.features.normalize_scores import normalize_scores
from tl.candidate_ranking.combine_linearly import combine_linearly
from tl.features.get_kg_links import get_kg_links
from tl.evaluation.join import Join
import tempfile
from glob import glob
import shutil


class Wikifier(object):

    def __init__(self):
        config = json.load(open('wikifier/config.json'))

        self.asciiiiii = set(string.printable)
        self.es_url = config['es_url']
        self.augmented_dwd_index = config['augmented_dwd_index']

        self.fuzzy_augmented = FuzzyAugmented(es_url=self.es_url,
                                              es_index=self.augmented_dwd_index,
                                              es_user=None,
                                              es_pass=None,
                                              properties="labels.en,labels.de,labels.es,labels.fr,labels.it,labels.nl,"
                                                         "labels.pl,labels.pt,labels.sv,aliases.en,aliases.de,"
                                                         "aliases.es,aliases.fr,aliases.it,aliases.nl,aliases.pl,"
                                                         "aliases.pt,aliases.sv,wikipedia_anchor_text.en,"
                                                         "wikitable_anchor_text.en,abbreviated_name.en,redirect_text.en",
                                              output_column_name='retrieval_score'
                                              )
        self.exact_match = ExactMatches(es_url=self.es_url, es_index=self.augmented_dwd_index)
        self.join = Join()
        self.temp_dir = tempfile.mkdtemp()
        self.auxiliary_fields = ['text_embedding', 'graph_embedding_complex']

    def wikify(self, i_df: pd.DataFrame, columns: str, debug: bool = False, k: int = 1):
        if debug:
            print('Step 1: Canonicalize')
        canonical_df = canonicalize(columns, output_column='label', df=i_df, add_context=True)

        if debug:
            print('Step 2: Clean')
        clean_df = clean('label', output_column='label_clean', df=canonical_df)

        if debug:
            print('Step 3: Get Fuzzy Augmented Candidates')
        fuzzy_augmented_candidates = self.fuzzy_augmented.get_matches(column='label_clean',
                                                                      df=clean_df,
                                                                      auxiliary_fields=self.auxiliary_fields,
                                                                      auxiliary_folder=self.temp_dir)

        if debug:
            print('Step 4: Get Exact Match Candidates')
        plus_exact_match_candidates = self.exact_match.get_exact_matches(column='label_clean',
                                                                         df=fuzzy_augmented_candidates,
                                                                         auxiliary_fields=self.auxiliary_fields,
                                                                         auxiliary_folder=self.temp_dir
                                                                         )
        # we have the text and graph embeddings for candidates, join the files and deduplicate them
        for aux_field in self.auxiliary_fields:
            aux_list = []
            for f in glob(f'{self.temp_dir}/*{aux_field}.tsv'):
                aux_list.append(pd.read_csv(f, sep='\t', dtype=object))
            aux_df = pd.concat(aux_list).drop_duplicates(subset=['qnode']).rename(columns={aux_field: 'embedding'})
            aux_df.to_csv(f'{self.temp_dir}/{aux_field}.tsv', sep='\t', index=False)

        # add features

        # 1. smallest qnode number
        if debug:
            print('Step 5: Smallest Qnode Feature')
        features_df = smallest_qnode_number(plus_exact_match_candidates)

        # 2. string similarity monge elkan
        if debug:
            print('Step 6: Monge Elkan Feature')
        me_ss = StringSimilarity(similarity_method=['monge_elkan:tokenizer=word'],
                                 ignore_case=True,
                                 output_column='monge_elkan',
                                 target_columns=['kg_labels', 'label_clean'],
                                 df=features_df)
        features_df = me_ss.get_similarity_score()

        # 3. string similarity jaccard
        if debug:
            print('Step 7: Jaccard Similarity Feature')
        jc_ss = StringSimilarity(similarity_method=['jaccard:tokenizer=word'],
                                 ignore_case=True,
                                 target_columns=['kg_descriptions', 'context'],
                                 output_column='des_cont_jaccard',
                                 df=features_df)
        features_df = jc_ss.get_similarity_score()

        # voting using the above features
        if debug:
            print('Step 8: Voting Feature')
        features_df = feature_voting(feature_col_names=['pagerank',
                                                        'smallest_qnode_number',
                                                        'monge_elkan',
                                                        'des_cont_jaccard'], df=features_df)

        # compute embedding score using column vector strategy
        if debug:
            print('Step 9: Score Using Embedding')
        embedding_vector = EmbeddingVector(kwargs={
            'df': features_df,
            'column_vector_strategy': 'centroid-of-singletons',
            'output_column_name': 'graph-embedding-score',
            'embedding_url': f'{self.es_url}/{self.augmented_dwd_index}/',
            'input_column_name': 'kg_id',
            'embedding_file': f'{self.temp_dir}/graph_embedding_complex.tsv',  # TODO fix this <-----
            'distance_function': 'cosine'
        })

        embedding_vector.get_vectors()
        embedding_vector.process_vectors()
        embedding_vector.add_score_column()
        features_df = embedding_vector.get_result_df()

        # normalize scores
        if debug:
            print('Step 10: Normalize Scores')
        normalized_df = normalize_scores(column='graph-embedding-score',
                                         norm_type='zscore',
                                         output_column='normalized-graph-embedding-score',
                                         df=features_df)

        normalized_df = normalize_scores(column='pagerank',
                                         norm_type='zscore',
                                         output_column='normalized-pagerank',
                                         df=normalized_df)

        normalized_df = normalize_scores(column='monge_elkan',
                                         norm_type='zscore',
                                         output_column='normalized-monge-elkan',
                                         df=normalized_df)

        if debug:
            print('Step 11: Combine Scores')
        combined_score_df = combine_linearly(weights='normalized-graph-embedding-score:1.0,'
                                                     'normalized-pagerank:1.0,'
                                                     'normalized-monge-elkan:1.0',
                                             output_column='ranking_score',
                                             df=normalized_df)

        if debug:
            print('Step 12: Get top ranked result')
            print(f'k:{k}')
        topk_df = get_kg_links('ranking_score', df=combined_score_df, label_column='label', top_k=k)

        if debug:
            print('Step 13: join with input file')
        output_df = self.join.join(topk_df, i_df, 'ranking_score')

        # delete the temp directoru
        shutil.rmtree(self.temp_dir)
        return output_df
