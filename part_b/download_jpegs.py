import pandas as pd
import os
import threading
import time
import mapillary.interface as mly
import download_jpegs_mapillary
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("MAPILLARY_ACCESS_TOKEN")
IN_CSV = os.getenv("IN_CSV")
OUT_FOLDER = os.getenv("OUT_FOLDER")

NUM_THREAD   = 100
CHUNK_SIZE   = 10000


def check_existing(folder):
    ids = set()
    print('Scanning for already-downloaded images...')
    for subdir, dirs, files in os.walk(folder):
        count = sum(1 for f in files if f.endswith('.jpeg'))
        if count:
            for f in files:
                if f.endswith('.jpeg'):
                    ids.add(f.replace('.jpeg', ''))
            print(f'  {count} images in {subdir}')
    print(f'Total already downloaded: {len(ids)}')
    return ids


if __name__ == '__main__':
    mly.set_access_token(ACCESS_TOKEN)
    Path(OUT_FOLDER).mkdir(parents=True, exist_ok=True)

    data = pd.read_csv(IN_CSV, low_memory=False).reset_index(drop=True)
    print(f'CSV loaded: {len(data)} rows')

    already_id = check_existing(OUT_FOLDER)
    print('Starting download...\n')

    chunks = [data.iloc[i:i+CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
    imgcnt = 0

    for chunk in chunks:
        start = chunk.index[0] + 1
        end   = chunk.index[-1] + 1
        out_sub = os.path.join(OUT_FOLDER, f'{start}_{end}')
        threads = []
        index   = 0

        for _, row in chunk.iterrows():
            uuid = row['uuid']
            if uuid in already_id:
                continue

            Path(out_sub).mkdir(parents=True, exist_ok=True)
            dst = os.path.join(out_sub, uuid + '.jpeg')
            index  += 1
            imgcnt += 1

            t = threading.Thread(
                target=download_jpegs_mapillary.download_image,
                args=(row['orig_id'], dst)
            )
            threads.append(t)

            if index % NUM_THREAD == 0:
                remaining = len(data) - len(already_id)
                print(f'Progress: {imgcnt}/{remaining} new  |  {len(already_id)} pre-existing')
                for t in threads:
                    t.daemon = True
                    t.start()
                threads[-1].join()
                time.sleep(0.3)
                threads = []

        remaining = len(data) - len(already_id)
        print(f'Progress: {imgcnt}/{remaining} new  |  {len(already_id)} pre-existing')
        try:
            for t in threads:
                t.daemon = True
                t.start()
            threads[-1].join()
        except (IndexError, UnboundLocalError):
            print('All images in this chunk already downloaded.')

    print('\nDone.')
