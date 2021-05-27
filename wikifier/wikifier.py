import os
import json
import shutil
import string
import tempfile
import pandas as pd
from glob import glob
from tl.features.tfidf import TFIDF
from tl.evaluation.join import Join
from tl.features import mosaic_features
from tl.features.get_kg_links import get_kg_links
from tl.candidate_ranking import predict_using_model
from tl.features.align_page_rank import align_page_rank
from tl.preprocess.preprocess import canonicalize, clean
from tl.features.normalize_scores import normalize_scores
from tl.features.external_embedding import EmbeddingVector
from tl.features.string_similarity import StringSimilarity
from tl.features.vote_by_classifier import vote_by_classifier
from tl.candidate_generation.get_exact_matches import ExactMatches
from tl.features.smallest_qnode_number import smallest_qnode_number
from tl.features.generate_reciprocal_rank import generate_reciprocal_rank
from tl.candidate_generation.get_fuzzy_augmented_matches import FuzzyAugmented
from tl.features.create_singleton_feature import create_singleton_feature


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

        _classifier_model_path = os.environ.get('CLASSIFIER_MODEL_PATH', None)
        self.classifier_model_path = _classifier_model_path if _classifier_model_path else config[
            'classifier_model_path']

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

        # align page rank
        if debug:
            print('Step 5: Align Page Rank')
        features_df = align_page_rank(df=plus_exact_match_candidates)

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

        # 4. string similarity jaro winkler
        if debug:
            print('Step 8: Jaro Winkler Similarity Feature')
        jw_ss = StringSimilarity(similarity_method=['jaro_winkler'],
                                 ignore_case=True,
                                 target_columns=['kg_labels', 'label_clean'],
                                 output_column='jaro_winkler',
                                 df=features_df)
        features_df = jw_ss.get_similarity_score()

        # levenshtein
        if debug:
            print('Step 9: Levenshtein Similarity Feature')
        lv_ss = StringSimilarity(similarity_method=['levenshtein'],
                                 ignore_case=True,
                                 target_columns=['kg_labels', 'label_clean'],
                                 output_column='levenshtein',
                                 df=features_df)
        features_df = lv_ss.get_similarity_score()

        # normalize scores
        if debug:
            print('Step 10: Normalize Scores')
        features_df = normalize_scores(column='des_cont_jaccard', df=features_df, norm_type='max_norm')

        # add singleton feature
        if debug:
            print('Step 11: Singleton Feature')
        features_df = create_singleton_feature(output_column='singleton', df=features_df)

        # smallest qnode number
        if debug:
            print('Step 12: Smallest Qnode Feature')
        features_df = smallest_qnode_number(features_df)

        # compute embedding score using column vector strategy

        # mosaic features
        if debug:
            print('Step 13: Mosaic Feature')
        features_df = mosaic_features.mosaic_features('kg_labels',
                                                      num_char=True,
                                                      num_tokens=True,
                                                      df=features_df)

        # vote by classifier
        if debug:
            print('Step 14: Vote by classifier')
        features_df = vote_by_classifier(self.classifier_model_path,
                                         df=features_df,
                                         prob_threshold=0.995)

        if debug:
            print('Step 15: Score Using Embedding')
        embedding_vector = EmbeddingVector(kwargs={
            'df': features_df,
            'column_vector_strategy': 'centroid-of-lof',
            'lof_strategy': 'ems-mv',
            'output_column_name': 'lof-graph-embedding-score',
            'embedding_url': f'{self.es_url}/{self.augmented_dwd_index}/',
            'input_column_name': 'kg_id',
            'embedding_file': f'{temp_dir}/graph_embedding_complex.tsv',
            'distance_function': 'cosine'
        })

        try:
            embedding_vector.get_vectors()
            embedding_vector.process_vectors()
            embedding_vector.add_score_column()
            features_df = embedding_vector.get_result_df()
        except Exception as e:
            print('Exception: {}'.format(e))
            features_df['lof-graph-embedding-score'] = 0.0

        # generate reciprocal rank feature
        if debug:
            print('Step 16: Generate Reciprocal Rank')
        features_df = generate_reciprocal_rank('lof-graph-embedding-score',
                                               'lof-reciprocal-rank',
                                               df=features_df)

        # add class count tfidf feature
        if debug:
            print('Step 17: Class TF IDF')
        ctfidf = TFIDF(output_column_name='lof_class_count_tf_idf_score',
                       feature_file=f'{temp_dir}/class_count.tsv',
                       feature_name='class_count',
                       total_docs=42123553,
                       singleton_column='is_lof',
                       df=features_df)

        features_df = ctfidf.compute_tfidf()

        # add property count tfidf feature
        if debug:
            print('Step 18: Property TF IDF')
        ptfidf = TFIDF(output_column_name='lof_property_count_tf_idf_score',
                       feature_file=f'{temp_dir}/property_count.tsv',
                       feature_name='property_count',
                       total_docs=42123553,
                       singleton_column='is_lof',
                       df=features_df)

        features_df = ptfidf.compute_tfidf()

        model_pred_df = predict_using_model.predict('siamese_prediction',
                                                    ranking_model=self.model_path,
                                                    min_max_scaler_path=self.min_max_scaler_path,
                                                    df=features_df)

        if debug:
            print('Step 19: Get top ranked result')
            print(f'k:{k}')
        topk_df = get_kg_links('siamese_prediction', df=model_pred_df, label_column='label', top_k=k)

        if debug:
            print('Step 20: join with input file')
        output_df = self.join.join(topk_df, i_df, 'ranking_score')

        # delete the temp directoru
        shutil.rmtree(temp_dir)
        return output_df
