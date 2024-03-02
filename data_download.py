import subprocess
from metaflow import S3
import os
import shutil
import boto3
from tempfile import TemporaryDirectory
try:
    import typer
    from rich.prompt import Prompt
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.text import Text
    from rich.console import Console
    from rich import print
    from rich.panel import Panel

except ImportError:
    # raise ImportError("Please install typer: pip install typer[all]")
    subprocess.run(["pip", "install", "typer[all]"], check=True)

ORIGINAL_HF_GENEFORMER_REPO = "https://huggingface.co/ctheodoris/Geneformer"
MODIFIED_GENEFORMER_REPO = "https://github.com/emattia/Geneformer.git"
METAFLOW_GENEFORMER_REPO = "https://github.com/emattia/metaflow-geneformer.git"
GENEFORMER_PATH = "/home/ob-workspace/Geneformer"
METAFLOW_GENEFORMER_PATH = "/home/ob-workspace/metaflow-geneformer"
DEFAULT_CONDA_ENV_NAME = "ob-geneformer"
PYTHON_BINARY_PATH = "/home/ob-workspace/.mambaforge/envs/{}/bin/python"
PIP_BINARY_PATH = "/home/ob-workspace/.mambaforge/envs/{}/bin/pip"
S3_ROOT = "s3://outerbounds-datasets"
S3_ROLE = "arn:aws:iam::006988687827:role/outerbounds-datasets-read-list-access" # NOTE: only for dev-content
S3_DATA_KEY = "Genecorpus-30M"
DATASET_SAMPLES = [
    # directories 
    "cell_type_train_data.dataset",
    "human_dcm_hcm_nf.dataset",
    "dosage_sensitive_tfs",
    "heart_atlas_endothelial_cells.dataset",
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

def _download_directory(download_path, store_key=""):
    final_path = os.path.join(S3_ROOT, store_key)
    os.makedirs(download_path, exist_ok=True)
    with S3(s3root=final_path, role=S3_ROLE) as s3:
        for s3obj in s3.get_all():
            move_path = os.path.join(download_path, s3obj.key)
            if not os.path.exists(os.path.dirname(move_path)):
                os.makedirs(os.path.dirname(move_path), exist_ok=True)
            shutil.move(s3obj.path, os.path.join(download_path, s3obj.key))

def download(download_path, store_key=""):
    if download_path.endswith(".csv") or download_path.endswith(".pkl"):
        with S3(s3root=S3_ROOT, role=S3_ROLE) as s3:
            obj = s3.get(store_key)
            with open(download_path, "wb") as f:
                f.write(obj.blob)
    else:
        _download_directory(download_path, store_key)

def run_command(command: str, progress: Progress = None):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if stdout:
        progress.console.print(stdout.decode("utf-8"))
    if stderr:
        progress.console.print(stderr.decode("utf-8"))

def main(conda_env_name: str = DEFAULT_CONDA_ENV_NAME):
    python_path = PYTHON_BINARY_PATH.format(conda_env_name)
    pip_path = PIP_BINARY_PATH.format(conda_env_name)

    panel = Panel(Text("\n Hello 👋 welcome to the Geneformer project 🧬🦠💻 \n", justify="center", style="bold green"))
    print(panel)
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:

        task = progress.add_task("Installing system dependencies...", total=None)
        print("Installing system dependencies...")
        run_command("sudo apt-get update && sudo apt-get install -y curl git git-lfs", progress)
        run_command("sudo apt-get update", progress)
        progress.update(task, completed=True)

        task2 = progress.add_task("Creating local dependencies environment...", total=None)
        run_command(f"mamba create -n {conda_env_name} python=3.10 pip -y", progress)
        run_command(f"conda activate {conda_env_name} && mamba install -n {conda_env_name} ipykernel --update-deps --force-reinstall -y", progress)
        progress.update(task2, completed=True)

        task3 = progress.add_task("Installing Geneformer dependencies...", total=None)
        run_command(f"git clone {ORIGINAL_HF_GENEFORMER_REPO} {GENEFORMER_PATH}", progress)
        run_command(f"cd {GENEFORMER_PATH} && {pip_path} install . && cd ..", progress)
        run_command(f"{pip_path} install outerbounds accelerate hyperopt typer[all]", progress)
        progress.update(task3, completed=True)

        task4 = progress.add_task("Downloading metaflow-geneformer tools...", total=None)
        print("Downloading metaflow-geneformer tools...")
        run_command(f"git clone {METAFLOW_GENEFORMER_REPO} {METAFLOW_GENEFORMER_PATH}", progress)
        progress.update(task4, completed=True)
        
        task5 = progress.add_task("Downloading modified notebooks from original geneformer...", total=None)
        with TemporaryDirectory() as tempdir:
            run_command(f"git clone {MODIFIED_GENEFORMER_REPO} {tempdir}", progress)
            run_command(f"cp -r {tempdir}/examples/* {GENEFORMER_PATH}/examples", progress)
        progress.update(task5, completed=True)
        
    big_data_response = Prompt.ask(
        "Do you want to download the big pre-training data file (122GB)? (y/n)",
        choices=["y", "n"], 
        default="n"
    )

    if big_data_response == "y":
        DATASET_SAMPLES.append("genecorpus_30M_2048.dataset")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        taskData = progress.add_task("Downloading data...", total=None)
        if not os.path.exists("data"):
            os.makedirs("data")
        for data_path in DATASET_SAMPLES:
            if not os.path.exists(os.path.join("data", data_path)):
                progress.console.print(f"Downloading {data_path}...")
                download(
                    download_path=os.path.join("data", data_path), 
                    store_key=os.path.join(S3_DATA_KEY, data_path)
                )
            else:
                progress.console.print(f"Skipping {data_path}, already downloaded.")
        progress.update(taskData, completed=True)
    

if __name__ == "__main__":
    typer.run(main)