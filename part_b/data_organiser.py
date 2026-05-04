import os
import pandas as pd
import shutil
from dotenv import load_dotenv


metadata_file = os.getenv('METADATA_FILE')
source_folder = os.getenv('SOURCE_FOLDER')
output_folder = os.getenv('OUTPUT_FOLDER')

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

df = pd.read_csv(metadata_file)



for index, row in df.iterrows():
    uuid = str(row['uuid']).strip()
    country = str(row['country']).strip()
    city = str(row['city']).strip()
    
    target_dir = os.path.join(output_folder, country, city)
    
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    
    found = False
    for ext in ['.jpeg','.jpg']:
        filename = f"{uuid}{ext}"
        src_path = os.path.join(source_folder, filename)
        dst_path = os.path.join(target_dir, filename)

        if os.path.exists(src_path):
            shutil.move(src_path, dst_path)
            print(f"Moved: {filename} -> {country}/{city}")
            found = True
            break # Stop looking for this UUID once found
            
    if not found:
        print(f"File not found for UUID: {uuid}")

print("Images are now organized by country and city.")