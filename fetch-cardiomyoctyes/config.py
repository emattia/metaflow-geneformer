GEO_URLS = [
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE156nnn/GSE156793/suppl/GSE156793_S1_metadata_cells.txt.gz",
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE156nnn/GSE156793/suppl/GSE156793_S2_Metadata_genes.txt.gz",
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE156nnn/GSE156793/suppl/GSE156793_S3_gene_count.loom.gz",
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE156nnn/GSE156793/suppl/GSE156793_S4_gene_expression_tissue.txt.gz",
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE156nnn/GSE156793/suppl/GSE156793_S5_gene_fraction_tissue.txt.gz",
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE156nnn/GSE156793/suppl/GSE156793_S6_gene_expression_celltype.txt.gz",
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE156nnn/GSE156793/suppl/GSE156793_S7_gene_fraction_celltype.txt.gz",
    "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE156nnn/GSE156793/suppl/GSE156793_S8_DE_gene_cells.csv.gz"
]

DATA_KEY = "GEO"
DATA_FILEPATH = "GSE156793_S3_gene_count.loom"

FILTER_FEATURE_NAME="Main_cluster_name"
FILTER_FEATURE_VALUE="Cardiomyocytes"
DATA_FILTERED_FILEPATH = f"GSE156793_S3_gene_count_{FILTER_FEATURE_VALUE.lower()}.loom"

# TODO IMAGE = "public.ecr.aws/outerbounds/geneformer:latest"

# number cpu cores
NUM_CPUS = 4