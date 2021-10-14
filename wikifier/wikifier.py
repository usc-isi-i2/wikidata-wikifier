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

        _model_path = os.environ.get('WIKIFIER_MODEL_PATH', None)
        self.model_path = _model_path if _model_path else config['model_path']

        _min_max_scaler_path = os.environ.get('WIKIFIER_MIN_MAX_SCALER_PATH', None)

        self.min_max_scaler_path = _min_max_scaler_path if _min_max_scaler_path else config["min_max_scaler_path"]
        self.features = ["monge_elkan", "monge_elkan_aliases", "jaro_winkler",
                         "levenshtein", "singleton", "context_score_3", "pgt_centroid_score",
                         "pgt_class_count_tf_idf_score",
                         "pgt_property_count_tf_idf_score", "num_occurences"]

        self.pseudo_gt_features = ["monge_elkan", "monge_elkan_aliases", "jaro_winkler",
                                   "levenshtein", "singleton", "pgr_rts", "context_score",
                                   "smc_class_score", "smc_property_score"]
        _pseudo_gt_model = os.environ.get('PSEUDO_GT_MODEL', None)
        self.pseudo_gt_model = _pseudo_gt_model if _pseudo_gt_model else config['pseudo_gt_model']

        _pseudo_g_min_max_scaler_path = os.environ.get('PSEUDO_GT_MIN_MAX_SCALER_PATH', None)

        self.pseudo_gt_min_max_scaler_path = _pseudo_g_min_max_scaler_path if _pseudo_g_min_max_scaler_path else config[
            'pseudo_gt_min_max_scaler_path']

    def wikify(self,
               i_df: pd.DataFrame,
               columns: str,
               output_path: str,
               debug: bool = False,
               k: int = 1,
               colorized_output: bool = False,
               isa: str = None) -> str:
        temp_dir = tempfile.mkdtemp()

        aux_path = f"{temp_dir}/aux_files"
        Path(aux_path).mkdir(parents=True, exist_ok=True)

        pipeline_temp_dir = f"{temp_dir}/temp"
        input_file_path = f"{temp_dir}/input.csv"
        candidate_file_path = f"{temp_dir}/candidates.csv"
        intermediate_file = f"{temp_dir}/intermediate.csv"
        output_file = f"{temp_dir}/colorized.xlsx" if colorized_output else f"{temp_dir}/output.csv"

        Path(pipeline_temp_dir).mkdir(parents=True, exist_ok=True)

        graph_embedding_complex_file = f"{aux_path}/graph_embedding_complex.tsv"
        class_count_file = f"{aux_path}/class_count.tsv"
        property_count_file = f"{aux_path}/property_count.tsv"
        context_file = f"{aux_path}/context.jl"
        context_property_file = f"{aux_path}/context_property.csv"

        i_df.to_csv(input_file_path, index=False)

        # candidate generation

        candidate_generation_command = f"tl canonicalize -c '{columns}' --add-context  {input_file_path} \
                                            / clean -c label -o label_clean \
                                            / --url {self.es_url} --index {self.augmented_dwd_index} \
                                            get-fuzzy-augmented-matches -c label_clean \
                                            --auxiliary-fields {self.auxiliary_fields} \
                                            --auxiliary-folder {pipeline_temp_dir} \
                                            / get-ngram-matches -c label_clean  \
                                            --auxiliary-fields {self.auxiliary_fields} \
                                            --auxiliary-folder {pipeline_temp_dir} \
                                            / get-trigram-matches -c label_clean \
                                            --auxiliary-fields {self.auxiliary_fields} \
                                            --auxiliary-folder {pipeline_temp_dir} \
                                            / get-exact-matches -c label_clean \
                                            --auxiliary-fields {self.auxiliary_fields} \
                                            --auxiliary-folder {pipeline_temp_dir} > {candidate_file_path}" \
            if isa is None else \
            f"tl canonicalize -c '{columns}' --add-context  {input_file_path} \
                                                   / clean -c label -o label_clean \
                                                   / --url {self.es_url} --index {self.augmented_dwd_index} \
                                                   get-fuzzy-augmented-matches -c label_clean \
                                                   --auxiliary-fields {self.auxiliary_fields} \
                                                   --auxiliary-folder {pipeline_temp_dir} \
                                                   --isa {isa} \
                                                   / get-ngram-matches -c label_clean  \
                                                   --auxiliary-fields {self.auxiliary_fields} \
                                                   --auxiliary-folder {pipeline_temp_dir} \
                                                   --isa {isa} \
                                                   / get-trigram-matches -c label_clean \
                                                   --auxiliary-fields {self.auxiliary_fields} \
                                                   --auxiliary-folder {pipeline_temp_dir} \
                                                   --isa {isa} \
                                                   / get-exact-matches -c label_clean \
                                                   --auxiliary-fields {self.auxiliary_fields} \
                                                   --auxiliary-folder {pipeline_temp_dir} \
                                                   --isa {isa} > {candidate_file_path}"

        cc_output = subprocess.getoutput(candidate_generation_command)
        if debug:
            print(cc_output)

        for field in self.auxiliary_fields.split(','):
            aux_list = []
            if field == 'context':
                file_list = glob(f'{pipeline_temp_dir}/*{field}.jl')
                o_f = open(context_file, 'w')

                for i_f_P in file_list:
                    i_f = open(i_f_P)
                    for line in i_f:
                        o_f.write(line)
                    i_f.close()
                o_f.close()

            else:
                for f in glob(f'{pipeline_temp_dir}/*{field}.tsv'):
                    aux_list.append(pd.read_csv(f, sep='\t', dtype=object))
                aux_df = pd.concat(aux_list).drop_duplicates(subset=['qnode'])
                if field == 'class_count':
                    aux_df.to_csv(class_count_file, sep='\t', index=False)
                elif field == 'property_count':
                    aux_df.to_csv(property_count_file, sep='\t', index=False)
                else:
                    aux_df.to_csv(graph_embedding_complex_file, sep='\t', index=False)

        # feature computation
        pgt_features_str = ",".join(self.pseudo_gt_features)
        features_str = ",".join(self.features)
        columns_to_color = f"{features_str},siamese_prediction"

        feature_computation_command = f"tl deduplicate-candidates -c kg_id {candidate_file_path} \
                                        / string-similarity -i --method symmetric_monge_elkan:tokenizer=word -o \
                                        monge_elkan --threshold 0.5 \
                                        / string-similarity -i --method symmetric_monge_elkan:tokenizer=word \
                                        -c label_clean kg_aliases -o monge_elkan_aliases --threshold 0.5 \
                                        / string-similarity -i --method jaro_winkler -o jaro_winkler --threshold 0.5 \
                                        / string-similarity -i --method levenshtein -o levenshtein --threshold 0.5 \
                                        / create-singleton-feature -o singleton \
                                        / pick-hc-candidates -o ignore_candidate \
                                        --string-similarity-label-columns monge_elkan,jaro_winkler,levenshtein \
                                        --string-similarity-alias-columns monge_elkan_aliases \
                                        / context-match --debug --context-file {context_file} \
                                        --ignore-column-name ignore_candidate -o context_score \
                                        --similarity-string-threshold 0.85 --similarity-quantity-threshold 0.9 \
                                        --save-relevant-properties --context-properties-path {context_property_file} \
                                        / kth-percentile -c context_score -o kth_percenter \
                                        --ignore-column ignore_candidate --k-percentile 0.75  --minimum-cells 10 \
                                        / pgt-semantic-tf-idf \
                                        -o smc_class_score \
                                        --pagerank-column pagerank \
                                        --retrieval-score-column retrieval_score \
                                        --feature-file {class_count_file} \
                                        --feature-name class_count \
                                        --high-confidence-column kth_percenter \
                                        / pgt-semantic-tf-idf \
                                        -o smc_property_score \
                                        --pagerank-column pagerank \
                                        --retrieval-score-column retrieval_score \
                                        --feature-file {property_count_file} \
                                        --feature-name property_count \
                                        --high-confidence-column kth_percenter \
                                        / predict-using-model -o pseudo_gt_prediction \
                                        --features {pgt_features_str} \
                                        --ranking-model {self.pseudo_gt_model} \
                                        --ignore-column ignore_candidate \
                                        --normalization-factor {self.pseudo_gt_min_max_scaler_path} \
                                        / create-pseudo-gt -o pseudo_gt \
                                        --column-thresholds pseudo_gt_prediction:mean \
                                        --filter smc_class_score:0 \
                                        / context-match --debug --context-file {context_file} -o context_score_3 \
                                        --similarity-string-threshold 0.85 --similarity-quantity-threshold 0.9 \
                                        --use-relevant-properties --context-properties-path {context_property_file} \
                                        / mosaic-features -c kg_labels --num-char --num-tokens \
                                        / score-using-embedding \
                                        --column-vector-strategy centroid-of-lof \
                                        --lof-strategy pseudo-gt \
                                        -o pgt_centroid_score \
                                        --embedding-file {graph_embedding_complex_file} \
                                        / compute-tf-idf  \
                                        --feature-file {class_count_file} \
                                        --feature-name class_count \
                                        --singleton-column pseudo_gt \
                                        -o pgt_class_count_tf_idf_score \
                                        / compute-tf-idf \
                                        --feature-file {property_count_file} \
                                        --feature-name property_count \
                                        --singleton-column pseudo_gt \
                                        -o pgt_property_count_tf_idf_score \
                                        / predict-using-model -o siamese_prediction \
                                        --features {features_str} \
                                        --ranking-model {self.model_path} \
                                        --normalization-factor {self.min_max_scaler_path} \
                                        > {intermediate_file}"

        fc_output = subprocess.getoutput(feature_computation_command)
        if debug:
            print(fc_output)

        if colorized_output:
            output_command = f"tl get-kg-links -c siamese_prediction -k {k}  \
                             --k-rows  {intermediate_file}\
                             / add-color -c {columns_to_color} -k {k} \
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
