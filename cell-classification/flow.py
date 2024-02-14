from metaflow import FlowSpec, step, kubernetes, current
import os
import sys
import subprocess
from utils import DataStore, ModelOps
from config import *


class CellClassificationFinetuning(FlowSpec, DataStore, ModelOps):
    """
    This workflow runs a finetuning of the geneformer model for a cell classification task.
    You can find the source code that is heavily used in the mixins for the class here:
        https://huggingface.co/ctheodoris/Geneformer/blob/main/examples/cell_classification.ipynb

    Workflow and training hyperparameters are set in config.py.
    """

    @step
    def start(self):
        s3_path = os.path.join(DATA_KEY, DATA_DIR)
        if not self.already_exists(s3_path):
            if not os.path.exists(DATA_DIR):
                sys.exit(DATA_NOT_FOUND_MESSAGE)
            self.upload(local_path=DATA_DIR, store_key=s3_path)
        self.next(self.preprocess_and_finetune)

    @kubernetes(gpu=NUM_GPUS, cpu=NUM_CPUS, image=IMAGE)
    @step
    def preprocess_and_finetune(self):
        from datasets import load_from_disk

        self.download(
            download_path=DATA_DIR, store_key=os.path.join(DATA_KEY, DATA_DIR)
        )
        train_dataset = load_from_disk(DATA_DIR)
        (
            trainset_dict,
            traintargetdict_dict,
            evalset_dict,
            organ_list,
        ) = self.preprocess(train_dataset)
        for organ in organ_list:
            organ_trainset = trainset_dict[organ]
            organ_evalset = evalset_dict[organ]
            organ_label_dict = traintargetdict_dict[organ]
            output_dir = self.finetune(
                organ,
                organ_trainset,
                organ_evalset,
                organ_label_dict,
                MODEL_CHECKPOINT_DIR,
            )
            self.upload(
                local_path=output_dir,
                store_key=os.path.join(DATA_KEY, str(current.run_id), output_dir),
            )
        self.next(self.end)

    @step
    def end(self):
        print("Flow is done!")


if __name__ == "__main__":
    CellClassificationFinetuning()