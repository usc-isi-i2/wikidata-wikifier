import os
import json
import shutil
import string
import tempfile
import subprocess
import pandas as pd
from glob import glob
from pathlib import Path


class Wikifier(object):

    def __init__(self):
        config = json.load(open('wikifier/config.json'))

        self.asciiiiii = set(string.printable)
        _es_url = os.environ.get('WIKIFIER_ES_URL', None)
        self.es_url = _es_url if _es_url else config['es_url']

        _es_index = os.environ.get('WIKIFIER_ES_INDEX', None)
        self.augmented_dwd_index = _es_index if _es_index else config['augmented_dwd_index']

        self.auxiliary_fields = "graph_embedding_complex,class_count,property_count,context"

        _classifier_model_path = os.environ.get('CLASSIFIER_MODEL_PATH', None)
        self.classifier_model_path = _classifier_model_path if _classifier_model_path else config[
            'classifier_model_path']

        _model_path = os.environ.get('WIKIFIER_MODEL_PATH', None)
        self.model_path = _model_path if _model_path else config['model_path']

        _min_max_scaler_path = os.environ.get('WIKIFIER_MIN_MAX_SCALER_PATH', None)

        _custom_context_path = os.environ.get('CUSTOM_CONTEXT_PATH', None)
        self.custom_context_path = _custom_context_path if _custom_context_path else config['custom_context_path']
        self.min_max_scaler_path = _min_max_scaler_path if _min_max_scaler_path else config["min_max_scaler_path"]
        self.features = ['pagerank', 'retrieval_score', 'monge_elkan', 'monge_elkan_aliases', 'des_cont_jaccard',
                         'jaro_winkler', 'levenshtein', 'singleton', 'num_char', 'num_tokens',
                         'lof_class_count_tf_idf_score', 'lof_property_count_tf_idf_score',
                         'lof-graph-embedding-score', 'lof-reciprocal-rank', 'context_score']
        self.classifier_features = ["aligned_pagerank", "smallest_qnode_number", "monge_elkan",
                                    "des_cont_jaccard_normalized"]

    def wikify(self,
               i_df: pd.DataFrame,
               columns: str,
               output_path: str,
               debug: bool = False,
               k: int = 1,
               colorized_output: bool = False,
               high_precision: bool = False) -> str:
        temp_dir = tempfile.mkdtemp()
        pipeline_temp_dir = f"{temp_dir}/temp"
        input_file_path = f"{temp_dir}/input.csv"
        candidate_file_path = f"{temp_dir}/candidates.csv"
        intermediate_file = f"{temp_dir}/intermediate.csv"
        output_file = f"{temp_dir}/colorized.xlsx" if colorized_output else f"{temp_dir}/output.csv"

        Path(pipeline_temp_dir).mkdir(parents=True, exist_ok=True)

        graph_embedding_complex_file = f"{pipeline_temp_dir}/graph_embedding_complex.tsv"
        class_count_file = f"{pipeline_temp_dir}/class_count.tsv"
        property_count_file = f"{pipeline_temp_dir}/property_count.tsv"
        context_file = f"{pipeline_temp_dir}/context.tsv"

        i_df.to_csv(input_file_path, index=False)

        # candidate generation

        candidate_generation_command = f"tl canonicalize -c '{columns}' --add-context  {input_file_path} \
                                            / clean -c label -o label_clean \
                                            / --url {self.es_url} --index {self.augmented_dwd_index} \
                                            get-fuzzy-augmented-matches -c label_clean \
                                            --auxiliary-fields {self.auxiliary_fields} \
                                            --auxiliary-folder {pipeline_temp_dir} \
                                            / --url {self.es_url} --index {self.augmented_dwd_index} \
                                            get-exact-matches \
                                            -c label_clean \
                                            --auxiliary-fields {self.auxiliary_fields} \
                                            --auxiliary-folder {pipeline_temp_dir} > {candidate_file_path}"
        cc_output = subprocess.getoutput(candidate_generation_command)
        if debug:
            print(cc_output)

        column_rename_dict = {
            'graph_embedding_complex': 'embedding',
            'class_count': 'class_count',
            'property_count': 'property_count',
            'context': 'context'
        }
        for field in self.auxiliary_fields.split(','):
            aux_list = []
            for f in glob(f'{pipeline_temp_dir}/*{field}.tsv'):
                aux_list.append(pd.read_csv(f, sep='\t'))
            aux_df = pd.concat(aux_list).drop_duplicates(subset=['qnode']).rename(
                columns={field: column_rename_dict[field]})
            aux_df.to_csv(f'{pipeline_temp_dir}/{field}.tsv', sep='\t', index=False)

        # feature computation
        features_str = ",".join(self.features)
        feature_computation_command = f"tl align-page-rank {candidate_file_path} \
                                        / string-similarity -i --method symmetric_monge_elkan:tokenizer=word \
                                        -o monge_elkan \
                                        / string-similarity -i --method symmetric_monge_elkan:tokenizer=word \
                                        -c label_clean kg_aliases -o monge_elkan_aliases \
                                        / string-similarity -i --method jaro_winkler -o jaro_winkler \
                                        / string-similarity -i --method levenshtein -o levenshtein \
                                        / string-similarity -i --method jaccard:tokenizer=word -c kg_descriptions \
                                        context -o des_cont_jaccard \
                                        / normalize-scores -c des_cont_jaccard / smallest-qnode-number \
                                        / mosaic-features -c kg_labels --num-char --num-tokens \
                                        / create-singleton-feature -o singleton \
                                        / vote-by-classifier  \
                                        --prob-threshold 0.995 \
                                        --model {self.classifier_model_path} \
                                        --features {','.join(self.classifier_features)} \
                                        / score-using-embedding \
                                        --column-vector-strategy centroid-of-lof \
                                        --lof-strategy ems-mv \
                                        -o lof-graph-embedding-score \
                                        --embedding-file {graph_embedding_complex_file} \
                                        --embedding-url {self.es_url}/{self.augmented_dwd_index}/ \
                                        / generate-reciprocal-rank  \
                                        -c lof-graph-embedding-score \
                                        -o lof-reciprocal-rank \
                                        / compute-tf-idf \
                                        --feature-file {class_count_file} \
                                        --feature-name class_count \
                                        --singleton-column is_lof \
                                        -o lof_class_count_tf_idf_score \
                                        / compute-tf-idf \
                                        --feature-file {property_count_file} \
                                        --feature-name property_count \
                                        --singleton-column is_lof \
                                        -o lof_property_count_tf_idf_score \
                                        / context-match \
                                        --context-file {context_file}  \
                                        -o context_score \
                                        --debug \
                                        / predict-using-model -o siamese_prediction \
                                        --ranking-model {self.model_path} \
                                        --features {features_str} \
                                        --normalization-factor {self.min_max_scaler_path} \
                                        > {intermediate_file}" if not high_precision else \
            f"tl align-page-rank {candidate_file_path} \
                                        / string-similarity -i --method symmetric_monge_elkan:tokenizer=word \
                                        -o monge_elkan \
                                        / string-similarity -i --method symmetric_monge_elkan:tokenizer=word \
                                        -c label_clean kg_aliases -o monge_elkan_aliases \
                                        / string-similarity -i --method jaro_winkler -o jaro_winkler \
                                        / string-similarity -i --method levenshtein -o levenshtein \
                                        / string-similarity -i --method jaccard:tokenizer=word -c kg_descriptions \
                                        context -o des_cont_jaccard \
                                        / normalize-scores -c des_cont_jaccard / smallest-qnode-number \
                                        / mosaic-features -c kg_labels --num-char --num-tokens \
                                        / create-singleton-feature -o singleton \
                                        / vote-by-classifier  \
                                        --prob-threshold 0.995 \
                                        --model {self.classifier_model_path} \
                                        --features {','.join(self.classifier_features)} \
                                        / score-using-embedding \
                                        --column-vector-strategy centroid-of-lof \
                                        --lof-strategy ems-mv \
                                        -o lof-graph-embedding-score \
                                        --embedding-file {graph_embedding_complex_file} \
                                        --embedding-url {self.es_url}/{self.augmented_dwd_index}/ \
                                        / generate-reciprocal-rank  \
                                        -c lof-graph-embedding-score \
                                        -o lof-reciprocal-rank \
                                        / compute-tf-idf \
                                        --feature-file {class_count_file} \
                                        --feature-name class_count \
                                        --singleton-column is_lof \
                                        -o lof_class_count_tf_idf_score \
                                        / compute-tf-idf \
                                        --feature-file {property_count_file} \
                                        --feature-name property_count \
                                        --singleton-column is_lof \
                                        -o lof_property_count_tf_idf_score \
                                        / context-match \
                                        --context-file {context_file}  \
                                        --custom-context-file {self.custom_context_path} \
                                        --string-separator ';' \
                                        -o context_score \
                                        --debug \
                                        / predict-using-model -o siamese_prediction \
                                        --ranking-model {self.model_path} \
                                        --features {features_str} \
                                        --normalization-factor {self.min_max_scaler_path} \
                                        > {intermediate_file}"

        fc_output = subprocess.getoutput(feature_computation_command)
        if debug:
            print(fc_output)

        if colorized_output:
            output_command = f"tl get-kg-links -c siamese_prediction -k {k}  \
                             --k-rows  {intermediate_file}\
                             / add-color -c siamese_prediction -k {k} \
                             --output {output_file}"
        else:
            output_command = f"tl get-kg-links -c siamese_prediction -k {k} --k-rows {intermediate_file} \
                               / join -c siamese_prediction -f {input_file_path} --extra-info \
                               > {output_file}"
        oo_output = subprocess.getoutput(output_command)

        if debug:
            print(oo_output)

        copy_command = f"cp {output_file} {output_path}"
        subprocess.getoutput(copy_command)

        shutil.rmtree(temp_dir)
        return output_file.split("/")[-1]
