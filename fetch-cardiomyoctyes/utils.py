from metaflow.metaflow_config import DATATOOLS_S3ROOT
from metaflow import S3
import os
import shutil
import subprocess
from config import *
from tempfile import TemporaryDirectory
import numpy as np


class DataStore:

    _store_root = DATATOOLS_S3ROOT

    @property
    def root(self):
        return self._store_root

    @staticmethod
    def _walk_directory(root):
        path_keys = []
        for path, subdirs, files in os.walk(root):
            for name in files:
                path_keys.append(
                    (
                        os.path.relpath(os.path.join(path, name), root),
                        os.path.join(path, name),
                    )
                )
        return path_keys

    def _upload_directory(self, local_path, store_key=""):
        final_path = os.path.join(self._store_root, store_key)
        with S3(s3root=final_path) as s3:
            s3.put_files(self._walk_directory(local_path))

    def already_exists(self, store_key=""):
        final_path = os.path.join(self._store_root, store_key)
        with S3(s3root=final_path) as s3:
            if len(s3.list_paths()) == 0:
                return False
        return True

    def _download_directory(self, download_path, store_key=""):
        """
        Parameters
        ----------
        download_path : str
            Path to the folder where the store contents will be downloaded
        store_key : str
            Key suffixed to the store_root to save the store contents to
        """
        final_path = os.path.join(self._store_root, store_key)
        os.makedirs(download_path, exist_ok=True)
        with S3(s3root=final_path) as s3:
            for s3obj in s3.get_all():
                move_path = os.path.join(download_path, s3obj.key)
                if not os.path.exists(os.path.dirname(move_path)):
                    os.makedirs(os.path.dirname(move_path), exist_ok=True)
                shutil.move(s3obj.path, os.path.join(download_path, s3obj.key))

    def upload(self, local_path, store_key=""):
        """
        Parameters
        ----------
        local_path : str
            Path to the store contents to be saved in cloud object storage.
        store_key : str
            Key suffixed to the store_root to save the store contents to.
        """
        if os.path.isdir(local_path):
            self._upload_directory(local_path, store_key)
        else:
            final_path = os.path.join(self._store_root, store_key)
            with S3(s3root=final_path) as s3:
                s3.put_files([(local_path, local_path)])

    def download(self, download_path, store_key=""):
        """
        Parameters
        ----------
        store_key : str
            Key suffixed to the store_root to download the store contents from
        download_path : str
            Path to the folder where the store contents will be downloaded
        """
        if not self.already_exists(store_key):
            raise ValueError(
                f"Model with key {store_key} does not exist in {self._store_root}"
            )
        self._download_directory(download_path, store_key)

    def download_file(self, download_path, store_key=""):
        """
        Parameters
        ----------
        store_key : str
            Key suffixed to the store_root to download the store contents from
        download_path : str
            Path to the file where the store contents will be downloaded
        """
        final_path = os.path.join(self._store_root, store_key)
        with S3(s3root=final_path) as s3:
            obj = s3.get(store_key)
            with open(download_path, "wb") as f:
                f.write(obj.blob)

    def _install_system_dependencies(self):
        "TODO: move this to docker image"
        subprocess.run(["sudo", "apt-get", "update"])
        subprocess.run(["sudo", "apt-get", "install", "-y", "wget", "gzip"])

    def fetch_data(self):
        os.makedirs(DATA_FILEPATH, exist_ok=True)
        for url in GEO_URLS:
            subprocess.run(["wget", url, "-P", DATA_FILEPATH], check=True)
            subprocess.run(["gzip", "-d", os.path.join(DATA_FILEPATH, url.split("/")[-1])], check=True)

class ModelOps:
    def filter(
        self, 
        loom_dataset_path=DATA_FILEPATH,
        feature_name=FILTER_FEATURE_NAME,  # feature to filter
        feature_value=FILTER_FEATURE_VALUE # value to filter for
    ):
        import loompy
        loom_dataset = loompy.connect(loom_dataset_path)
        cluster_name = loom_dataset.ca[feature_name]
        is_cardio = cluster_name == feature_value
        cardio_col_indices = np.where(is_cardio)[0].tolist()
        with loompy.new(DATA_FILTERED_FILEPATH) as loom_dataset_out:
            for (ix, selection, view) in loom_dataset.scan(items=np.array(cardio_col_indices), axis=1):
                loom_dataset_out.add_columns(view.layers, col_attrs=view.ca, row_attrs=view.ra)
        del loom_dataset_out
        return loompy.connect(DATA_FILTERED_FILEPATH)

    def build_features(
        self, 
        ds, 
        ensembl_ids_colname="gene_id", # may be referred to as Accession
        total_reads_colname="All_reads"
    ):
        "Prepare the dataset to be tokenized for geneformer."
        trim_func = lambda x: x.split(".")[0]
        ensembl_ids = pd.Series(ds.ra[ensembl_ids_colname]).apply(trim_func)
        ds.ra['ensembl_id'] = ensembl_ids.values
        ds.ca['n_counts'] = ds.ca[total_reads_colname]
        return ds
        