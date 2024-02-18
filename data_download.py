from metaflow.metaflow_config import DATATOOLS_S3ROOT
from metaflow import S3
import os
import shutil
import boto3

def _download_directory(download_path, store_key=""):
    final_path = os.path.join(DATATOOLS_S3ROOT, store_key)
    os.makedirs(download_path, exist_ok=True)
    with S3(s3root=final_path) as s3:
        for s3obj in s3.get_all():
            move_path = os.path.join(download_path, s3obj.key)
            if not os.path.exists(os.path.dirname(move_path)):
                os.makedirs(os.path.dirname(move_path), exist_ok=True)
            shutil.move(s3obj.path, os.path.join(download_path, s3obj.key))

def download(download_path, store_key=""):
    if download_path.endswith(".csv") or download_path.endswith(".pkl"):
        s3 = boto3.client('s3')
        s3_root = DATATOOLS_S3ROOT[5:]
        bucket = s3_root.split("/")[0]
        prefix = "/".join(s3_root.split("/")[1:])
        s3.download_file(bucket, os.path.join(prefix, store_key), download_path)
    else:
        _download_directory(download_path, store_key)

S3_DATA_KEY = "Genecorpus-30M"
DATASET_SAMPLES = [
    # directories
    "cell_type_train_data.dataset",
    "human_dcm_hcm_nf.dataset",
    "dosage_sensitive_tfs",
    "heart_atlas_endothelial_cells.dataset",
    "human_dcm_hcm_nf.dataset",
    "notch1_network",
    "tf_regulatory_range",
    "tf_targets",
    # single files
    "gene_info_table.csv",
    "genecorpus_30M_2048_lengths.pkl",
    "genecorpus_30M_2048_sorted_lengths.pkl",
    "token_dictionary.pkl",
    # big files
    "genecorpus_30M_2048.dataset"
]

if not os.path.exists("data"):
    os.makedirs("data")

for data_path in DATASET_SAMPLES:
    if not os.path.exists(os.path.join("data", data_path)):
        download(
            download_path=os.path.join("data", data_path), 
            store_key=os.path.join(S3_DATA_KEY, data_path)
        )