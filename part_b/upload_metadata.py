import os
from huggingface_hub import HfApi
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
HF_REPO_ID = "cookekieran/eu_citiesv2"
TRAIN_PATH = os.getenv(r"TRAIN_METADATA_PATH")
TEST_PATH = os.getenv(r"TEST_METADATA_PATH")
HF_TOKEN = os.getenv("HF_TOKEN")

api = HfApi()

# 1. Upload Training CSV
api.upload_file(
    path_or_fileobj=TRAIN_PATH,
    path_in_repo="train_metadata.csv",
    repo_id=HF_REPO_ID,
    repo_type="dataset",
    token=HF_TOKEN
)

# 2. Upload Testing CSV
api.upload_file(
    path_or_fileobj=TEST_PATH,
    path_in_repo="test_metadata.csv",
    repo_id=HF_REPO_ID,
    repo_type="dataset",
    token=HF_TOKEN
)

print("CSVs uploaded.")