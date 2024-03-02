from metaflow import FlowSpec, step, kubernetes, current
import os
import sys
import subprocess
from utils import DataStore, ModelOps
from config import *


class InSilicoPerturbation(FlowSpec, DataStore, ModelOps):
    """
    This workflow runs a finetuning of the geneformer model for a cell classification task.
    You can find the source code that is heavily used in the mixins for the class here:
        https://huggingface.co/ctheodoris/Geneformer/blob/main/examples/cell_classification.ipynb

    Workflow and training hyperparameters are set in config.py.
    """

    # @kubernetes(cpu=4, image=IMAGE)
    @step
    def start(self):
        s3_path = os.path.join(S3_DATA_KEY)
        if not self.already_exists(s3_path):
            if not os.path.exists(LOCAL_DATA_DIR):
                sys.exit(DATA_NOT_FOUND_MESSAGE)
            self.upload(local_path=LOCAL_DATA_DIR, store_key=s3_path)
        self.next(self.preprocess)

    # @kubernetes(cpu=N_CPU, gpu=N_GPU, image=IMAGE)
    @step
    def preprocess(self):
        import pandas as pd
        # self.download(download_path=LOCAL_DATA_DIR, store_key=S3_DATA_KEY)
        self.perturb_data(self.compute_cell_embeddings())
        self.df = pd.read_csv(os.path.join(OUTPUT_STATS_PATH, f'{OUTPUT_PREFIX}.csv'))
        self.next(self.end) 

    @property
    def downstream_use_msg(self):
        return """
        from metaflow import Run
        run = Run({}/{})
        df = run.data.df
        """

    @step
    def end(self):
        print("Flow is done!")
        print('\n')
        print(self.downstream_use_msg.format(current.flow_name, current.run_id))


if __name__ == "__main__":
    InSilicoPerturbation()