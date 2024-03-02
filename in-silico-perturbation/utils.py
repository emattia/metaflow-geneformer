from metaflow.metaflow_config import DATATOOLS_S3ROOT
from metaflow import S3
import os
import shutil
import subprocess
from config import *
from tempfile import TemporaryDirectory


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
                # create a tuple of (key, path)
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

    def upload_hf_dataset(self, dataset, store_key=""):
        """
        Parameters
        ----------
        dataset : datasets.Dataset
            Huggingface dataset to be saved in cloud object storage.
        store_key : str
            Key suffixed to the store_root to save the store contents to.
        """
        with TemporaryDirectory() as temp_dir: 
            # OK for small data. For big data, use a different method.
            dataset.save_to_disk(temp_dir)
            self.upload(temp_dir, store_key)


class ModelOps:

    @property
    def filter_data_dict(self):
        return {"cell_type": ["Cardiomyocyte1","Cardiomyocyte2","Cardiomyocyte3"]}

    @property
    def cell_states_to_model(self): 
        # first obtain start, goal, and alt embedding positions
        # this function was changed to be separate from perturb_data
        # to avoid repeating calcuations when parallelizing perturb_data
        return {
            "state_key": "disease", 
            "start_state": "dcm", 
            "goal_state": "nf", 
            "alt_states": ["hcm"]
        }

    def compute_cell_embeddings(self):

        from geneformer import EmbExtractor

        embex = EmbExtractor(
            model_type="CellClassifier",
            num_classes=3,
            filter_data=self.filter_data_dict,
            max_ncells=MAX_NCELLS,
            emb_layer=EMB_LAYER,
            summary_stat="exact_mean",
            forward_batch_size=FORWARD_BATCH_SIZE,
            nproc=16
        )

        state_embs_dict = embex.get_state_embs(
            self.cell_states_to_model,
            PRETRAINED_MODEL_PATH,
            LOCAL_DATA_DIR,
            OUTPUT_PATH,
            OUTPUT_PREFIX
        )

        return state_embs_dict

    def perturb_data(
        self, 
        state_embs_dict,
        mode = "goal_state_shift",
        genes_perturbed = "all"
    
    ):

        from geneformer import InSilicoPerturber
        from geneformer import InSilicoPerturberStats

        isp = InSilicoPerturber(
            perturb_type="delete",
            perturb_rank_shift=None,
            genes_to_perturb="all",
            combos=0,
            anchor_gene=None,
            model_type="CellClassifier",
            num_classes=3,
            emb_mode="cell",
            cell_emb_style="mean_pool",
            filter_data=self.filter_data_dict,
            cell_states_to_model=self.cell_states_to_model,
            state_embs_dict=state_embs_dict,
            max_ncells=MAX_NCELLS,
            emb_layer=EMB_LAYER,
            forward_batch_size=FORWARD_BATCH_SIZE,
            nproc=16
        )

        # outputs intermediate files from in silico perturbation
        isp.perturb_data(
            PRETRAINED_MODEL_PATH,
            LOCAL_DATA_DIR,
            OUTPUT_PATH,
            OUTPUT_PREFIX
        )

        ispstats = InSilicoPerturberStats(
            mode=mode,
            genes_perturbed=genes_perturbed,
            combos=0,
            anchor_gene=None,
            cell_states_to_model=self.cell_states_to_model
        )

        ispstats.get_stats(
            OUTPUT_PATH,
            None,
            OUTPUT_STATS_PATH,
            OUTPUT_PREFIX
        )