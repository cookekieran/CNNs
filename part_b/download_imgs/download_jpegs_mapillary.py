"""
This script contains functions imported by download_jpegs.py.
It can also be run on its own to download Mapillary images.

Input format requirement: a csv file with each row representing an image to download and
containing minimally three columns to specify its 'uuid' (the uuid assigned to the image),
'source' (whether its source is 'Mapillary' or 'KartaView'), and 'orig_id' (original ID as
given by the source).
"""

import pandas as pd
import os
import urllib
import threading
import mapillary.interface as mly
import time
import random
from pathlib import Path
from dotenv import load_dotenv

def download_image_from_url(image_url, dst_path):
    try:
        with urllib.request.urlopen(image_url) as web_file:
            data = web_file.read()
            with open(dst_path, mode='wb') as local_file:
                local_file.write(data)
    except urllib.error.URLError as e:
        print('network error', e)


def get_image_url(image_id):
    '''
    automatically download image for each row in the dataframe and append the image filename to the dataframe
    '''
    try:
        random_t = random.randint(1, 10)/10
        time.sleep(random_t)
        image_url = mly.image_thumbnail(image_id, 2048)
        return image_url
        # print('Successed')
        # return os.path.basename(url_2048)
    except Exception as e:
        print('network error', e)

def download_image(image_id, dst_path):
    image_url = get_image_url(image_id)
    
    if image_url is not None:
        download_image_from_url(image_url, dst_path)
    else:
        print(f"URL not found for image {image_id}")


def check_id(image_folder):
    ids = set()
    for name in os.listdir(image_folder):
        if name != '.DS_Store':
            ids.add(name.split('.')[0])
    return ids


if __name__ == '__main__':

    load_dotenv()
    access_token = os.getenv('MAPILLARY_ACCESS_TOKEN')
    output_path = os.getenv('OUTPUT_PATH')

    mly.set_access_token(access_token)

    # Update in_csvPath and out_jpegFolder to suit your needs
    in_csvPath = '10000_imgs.csv' # input csv
    out_jpegFolder = output_path # output folder to store the downloaded images
    Path(out_jpegFolder).mkdir(parents=True, exist_ok=True)

    threads = []
    num_thread = 10
    already_id = check_id(out_jpegFolder)

    data_l = pd.read_csv(in_csvPath)
    # data_l['orig_id'] = data_l['orig_id'].astype(float).apply(lambda x: '{:.0f}'.format(x)) # strip decimal points?

    data_l = data_l[data_l['source']=='Mapillary']

    index = 0

    for _, values in data_l.iterrows():
        image_id = values['uuid']
        if str(image_id) in already_id:
            continue

        uuid = values['uuid']
        dst_path = os.path.join(out_jpegFolder, uuid + '.jpeg')
        index += 1
        if index % num_thread == 0:
            print('Now:', index, len(data_l)-len(already_id),
                  'already:', index + len(already_id))
            t = threading.Thread(target=download_image,
                                 args=(image_id, dst_path,))
            threads.append(t)
            for t in threads:
                t.setDaemon(True)
                t.start()
            t.join()
            time.sleep(0.1)
            threads = []
        else:
            t = threading.Thread(target=download_image,
                                 args=(image_id, dst_path,))
            threads.append(t)

    for t in threads:
        t.setDaemon(True)
        t.start()
    t.join()


"""
orig_id numbers are not matching the numbers in the mapillary database. big numbers are being rounded automatically by excel.
"""