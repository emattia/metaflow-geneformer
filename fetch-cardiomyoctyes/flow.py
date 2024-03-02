from metaflow import FlowSpec, step, kubernetes, current
import os
import sys
import subprocess
from utils import DataStore, ModelOps
from config import *


class FetchCardiomyocitesData(FlowSpec, DataStore, ModelOps):

    """
    This workflow fetches data used for in silico perturbation in the orignial paper.
    The original file comes from the Gene Expression Omnibus - https://www.ncbi.nlm.nih.gov/geo/.

    The dataset id is GSE156793 - https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE156793
    The data is a Loom file (a special kind of HDF5 file) with a single-cell RNA-seq dataset of fetal gene expression.

    There are many different cell origins, and the goal of this workflow is to do the following steps:
        - Fetch the original copy and populate a cache in S3 storage (if it doesn't exist)
        - Preprocess the data by filtering the for only cells that are cardiomyocites
        - Split the data into training and evaluation sets and store them in S3
    """

    @step
    def start(self):
        s3_path = os.path.join(DATA_KEY, DATA_FILEPATH)
        if not self.already_exists(s3_path):
            if not os.path.exists(DATA_FILEPATH):
                self.fetch_data()
            self.upload(local_path=DATA_FILEPATH, store_key=s3_path)
        self.next(self.preprocess)

    @step
    def preprocess(self):
        try:
            import loompy 
        except ImportError:
            subprocess.run([sys.executable, "-m", "pip", "install", "loompy"], check=True)
            import loompy
        self.download_file(
            download_path=DATA_FILEPATH, 
            store_key=os.path.join(DATA_KEY, DATA_FILEPATH)
        )
        ds = loompy.connect(DATA_FILEPATH)
        cardiomyocite_ds = self.build_features(self.filter(ds))


        self.next(self.end) 

    @step
    def end(self):
        print("Flow is done!")


if __name__ == "__main__":
    FetchCardiomyocitesData()