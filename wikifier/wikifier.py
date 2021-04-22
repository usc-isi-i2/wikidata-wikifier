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
from tl.features.get_kg_links import get_kg_links
from tl.evaluation.join import Join
from tl.features.create_singleton_feature import create_singleton_feature
from wikifier.contrastive_model import PairwiseNetwork
import torch
from tl.features.tfidf import TFIDF
import tempfile
from glob import glob
import shutil
import pickle
import os


class Wikifier(object):

    def __init__(self):
        config = json.load(open('wikifier/config.json'))

        self.asciiiiii = set(string.printable)
        _es_url = os.environ.get('WIKIFIER_ES_URL', None)
        self.es_url = _es_url if _es_url else config['es_url']

        _es_index = os.environ.get('WIKIFIER_ES_INDEX', None)
        self.augmented_dwd_index = _es_index if _es_index else config['augmented_dwd_index']

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
        self.auxiliary_fields = ['graph_embedding_complex', 'class_count', 'property_count']
        _model_path = os.environ.get('WIKIFIER_MODEL_PATH', None)
        self.model_path = _model_path if _model_path else config['model_path']
        _min_max_scaler_path = os.environ.get('WIKIFIER_MIN_MAX_SCALER_PATH', None)
        self.min_max_scaler_path = _min_max_scaler_path if _min_max_scaler_path else config["min_max_scaler_path"]

    def wikify(self, i_df: pd.DataFrame, columns: str, debug: bool = False, k: int = 1):
        temp_dir = tempfile.mkdtemp()
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
                                                                      auxiliary_folder=temp_dir)

        if debug:
            print('Step 4: Get Exact Match Candidates')
        plus_exact_match_candidates = self.exact_match.get_exact_matches(column='label_clean',
                                                                         df=fuzzy_augmented_candidates,
                                                                         auxiliary_fields=self.auxiliary_fields,
                                                                         auxiliary_folder=temp_dir
                                                                         )

        plus_exact_match_candidates.to_csv('/tmp/candidates.tsv', sep='\t', index=False)
        # we have the text and graph embeddings for candidates, join the files and deduplicate them
        # for aux_field in self.auxiliary_fields:
        #     aux_list = []
        #     for f in glob(f'{temp_dir}/*{aux_field}.tsv'):
        #         aux_list.append(pd.read_csv(f, sep='\t', dtype=object))
        #     aux_df = pd.concat(aux_list).drop_duplicates(subset=['qnode']).rename(columns={aux_field: 'embedding'})
        #     aux_df.to_csv(f'{temp_dir}/{aux_field}.tsv', sep='\t', index=False)

        column_rename_dict = {
            'graph_embedding_complex': 'embedding',
            'class_count': 'class_count',
            'property_count': 'property_count'
        }
        for field in self.auxiliary_fields:
            aux_list = []
            for f in glob(f'{temp_dir}/*{field}.tsv'):
                aux_list.append(pd.read_csv(f, sep='\t', dtype=object))
            aux_df = pd.concat(aux_list).drop_duplicates(subset=['qnode']).rename(
                columns={field: column_rename_dict[field]})
            aux_df.to_csv(f'{temp_dir}/{field}.tsv', sep='\t', index=False)

        # add features

        # # 1. smallest qnode number
        # if debug:
        #     print('Step 5: Smallest Qnode Feature')
        # features_df = smallest_qnode_number(plus_exact_match_candidates)

        # 2. string similarity monge elkan
        if debug:
            print('Step 6: Monge Elkan Feature')
        me_ss = StringSimilarity(similarity_method=['monge_elkan:tokenizer=word'],
                                 ignore_case=True,
                                 output_column='monge_elkan',
                                 target_columns=['kg_labels', 'label_clean'],
                                 df=plus_exact_match_candidates)
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

        # 4. string similarity jaro winkler
        if debug:
            print('Step 8: Jaro Winkler Similarity Feature')
        jw_ss = StringSimilarity(similarity_method=['jaro_winkler'],
                                 ignore_case=True,
                                 target_columns=['kg_labels', 'label_clean'],
                                 output_column='jaro_winkler',
                                 df=features_df)
        features_df = jw_ss.get_similarity_score()

        # add singleton feature
        features_df = create_singleton_feature(output_column='singleton', df=features_df)

        # voting using the above features
        # if debug:
        #     print('Step 9: Voting Feature')
        # features_df = feature_voting(feature_col_names=['pagerank',
        #                                                 'smallest_qnode_number',
        #                                                 'monge_elkan',
        #                                                 'des_cont_jaccard'], df=features_df)

        # compute embedding score using column vector strategy
        if debug:
            print('Step 10: Score Using Embedding')
        embedding_vector = EmbeddingVector(kwargs={
            'df': features_df,
            'column_vector_strategy': 'centroid-of-singletons',
            'output_column_name': 'graph-embedding-score',
            'embedding_url': f'{self.es_url}/{self.augmented_dwd_index}/',
            'input_column_name': 'kg_id',
            'embedding_file': f'{temp_dir}/graph_embedding_complex.tsv',
            'distance_function': 'cosine'
        })

        embedding_vector.get_vectors()
        embedding_vector.process_vectors()
        embedding_vector.add_score_column()
        features_df = embedding_vector.get_result_df()

        # Additional features for Model Prediction

        features_df['num_char'] = features_df['kg_labels'].apply(lambda x: len(x) if not (pd.isna(x)) else 0)
        features_df['num_tokens'] = features_df['kg_labels'].apply(lambda x: len(x.split()) if not (pd.isna(x)) else 0)
        features_df = Wikifier.generate_reciprocal_rank(features_df)

        # add class count tfidf feature
        ctfidf = TFIDF(output_column_name='class_count_tf_idf_score',
                       feature_file=f'{temp_dir}/class_count.tsv',
                       feature_name='class_count',
                       total_docs=42123553,
                       singleton_column='singleton',
                       df=features_df)

        features_df = ctfidf.compute_tfidf()

        # add property count tfidf feature
        ptfidf = TFIDF(output_column_name='property_count_tf_idf_score',
                       feature_file=f'{temp_dir}/property_count.tsv',
                       feature_name='property_count',
                       total_docs=42123553,
                       singleton_column='singleton',
                       df=features_df)

        features_df = ptfidf.compute_tfidf()

        # Use pretrained model for prediction
        normalize_features = ['pagerank', 'retrieval_score', 'monge_elkan', 'des_cont_jaccard',
                              'jaro_winkler', 'graph-embedding-score', 'singleton',
                              'num_char', 'num_tokens', 'reciprocal_rank',
                              'class_count_tf_idf_score', 'property_count_tf_idf_score']

        # model = pickle.load(open(self.model_path, 'rb'))
        # model_df = features_df[features_list_model]
        # predicted_score = model.predict(model_df)
        # features_df['model_prediction'] = predicted_score

        model = PairwiseNetwork(12)
        model.load_state_dict(torch.load(self.model_path))
        scaler = pickle.load(open(self.min_max_scaler_path, 'rb'))

        grouped_obj = features_df.groupby(['row', 'column'])
        new_df_list = []
        pred = []
        for cell in grouped_obj:
            cell[1][normalize_features] = scaler.transform(cell[1][normalize_features])
            sorted_df = cell[1].sort_values('graph-embedding-score', ascending=False)[:64]
            sorted_df_features = sorted_df[normalize_features]
            new_df_list.append(sorted_df)
            arr = sorted_df_features.to_numpy()
            test_inp = []
            for a in arr:
                test_inp.append(a)
            test_tensor = torch.tensor(test_inp).float()
            scores = model.predict(test_tensor)
            pred.extend(torch.squeeze(scores).tolist())
        model_pred_df = pd.concat(new_df_list)
        model_pred_df['siamese_pred'] = pred
        # test_df.to_csv(output_table, index=False)

        if debug:
            print('Step 11: Get top ranked result')
            print(f'k:{k}')
        topk_df = get_kg_links('siamese_pred', df=model_pred_df, label_column='label', top_k=k)

        if debug:
            print('Step 12: join with input file')
        output_df = self.join.join(topk_df, i_df, 'ranking_score')

        # delete the temp directoru
        shutil.rmtree(temp_dir)
        return output_df

    @staticmethod
    def generate_reciprocal_rank(df: pd.DataFrame):
        final_list = []
        grouped_obj = df.groupby(['row', 'column'])
        for cell in grouped_obj:
            reciprocal_rank = list(1 / cell[1]['graph-embedding-score'].rank())
            cell[1]['reciprocal_rank'] = reciprocal_rank
            final_list.extend(cell[1].to_dict(orient='records'))
        odf = pd.DataFrame(final_list)
        return odf
