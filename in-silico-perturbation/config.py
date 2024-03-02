import os

S3_DATA_KEY = "Genecorpus-30M/human_dcm_hcm_nf.dataset"
LOCAL_DATA_DIR = "/home/ob-workspace/data/human_dcm_hcm_nf.dataset"

DATA_NOT_FOUND_MESSAGE = f"""Data not found in the {LOCAL_DATA_DIR} directory, and not found in the S3 cache.
Please download the data from https://huggingface.co/datasets/ctheodoris/Genecorpus-30M/tree/main/example_input_files/cell_classification/disease_classification/human_dcm_hcm_nf.dataset and place it in the working directory inside of the {LOCAL_DATA_DIR} folder."""

IMAGE = "public.ecr.aws/outerbounds/geneformer:latest"
N_CPU = 16
N_GPU = 1

# hyperparameters
MAX_NCELLS = 1
EMB_LAYER = 0
FORWARD_BATCH_SIZE = 1

PRETRAINED_MODEL_PATH = '/home/ob-workspace/Geneformer'
OUTPUT_PATH = 'in-silico-perturbation-output'
OUTPUT_PREFIX = 'fetal_gene_expression_cardiomyocite'
OUTPUT_STATS_PATH = 'in-silico-perturbation-output-stats'

if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)
if not os.path.exists(OUTPUT_STATS_PATH):
    os.makedirs(OUTPUT_STATS_PATH)