import os
import io
from PIL import Image
from datasets import Dataset, Features, Image as ImageFeature
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---

INPUT_FOLDER = os.getenv(r"DATA_INPUT_FOLDER")
HF_TOKEN = os.getenv("HF_TOKEN")
HF_REPO_ID = "cookekieran/eu_citiesv2" 
TARGET_SIZE = (224, 224)

SPLIT = "test"

def process_single_image(filename):
    img_path = os.path.join(INPUT_FOLDER, filename)
    try:
        with Image.open(img_path) as img:
            resized_img = img.convert("RGB").resize(TARGET_SIZE, Image.Resampling.LANCZOS)
            img_byte_arr = io.BytesIO()
            resized_img.save(img_byte_arr, format='JPEG', quality=85)
            return {"image": img_byte_arr.getvalue(), "file_name": filename}
    except Exception as e:
        return None

def full_upload():
    if not os.path.exists(INPUT_FOLDER):
        print(f"Error: Path not found!")
        return

    valid_extensions = ('.png', '.jpg', '.jpeg', '.webp')
    all_files = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith(valid_extensions)]
    
    print(f"Starting Multi-core processing for {len(all_files)} images...")
    
    final_data = {"image": [], "file_name": []}
    
    # use multiple cpu cores
    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(process_single_image, all_files), total=len(all_files), desc="Parallel Resizing"))

    # ignore 'None' files
    for res in results:
        if res:
            final_data["image"].append(res["image"])
            final_data["file_name"].append(res["file_name"])

    print("\nCreating Dataset...")
    dataset = Dataset.from_dict(final_data).cast_column("image", ImageFeature())

    print(f"Attempting upload to {HF_REPO_ID}...")
    try:
        dataset.push_to_hub(HF_REPO_ID, split=SPLIT, token=HF_TOKEN) # change for training/testing
        print(f"\nSUCCESS! All images are live at: https://huggingface.co/datasets/{HF_REPO_ID}")
    except Exception as e:
        print(f"\nUpload failed: {e}")

if __name__ == "__main__":
    full_upload()
