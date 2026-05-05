import os
from PIL import Image
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FOLDER = "INPUT_FOLDER" # Change as required
OUTPUT_FOLDER = "compressed_images"
TARGET_SIZE = (224, 224) # requirement for MobileNetV3

def compress_and_resize():
    # Create the output directory if it doesn't exist
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    # Filter for standard image formats
    valid_extensions = ('.jpg', '.jpeg', '.png')
    
    try:
        files_to_process = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith(valid_extensions)]
    except FileNotFoundError:
        print(f"Error: Could not find the folder '{INPUT_FOLDER}'. Please check the name.")
        return
        
    print(f"Found {len(files_to_process)} images. Firing up the compressor...")
    

    for filename in tqdm(files_to_process, desc="Resizing Images"):
        input_path = os.path.join(INPUT_FOLDER, filename)
        output_path = os.path.join(OUTPUT_FOLDER, filename)
        
        try:
            with Image.open(input_path) as img:
                # ensure RGB
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Resize using LANCZOS for high-quality downsampling
                img_resized = img.resize(TARGET_SIZE, Image.Resampling.LANCZOS)
                
                # Save as JPEG with 85% quality (excellent balance of size and visual fidelity)
                img_resized.save(output_path, "JPEG", quality=85)
                
        except Exception as e:
            print(f"\nSkipping {filename} due to error: {e}")

if __name__ == "__main__":
    compress_and_resize()
    print("\nBatch compression complete")
