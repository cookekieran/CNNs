import os
import urllib
import threading
import time
import random
import mapillary.interface as mly


def download_image_from_url(image_url, dst_path):
    try:
        with urllib.request.urlopen(image_url) as web_file:
            data = web_file.read()
            with open(dst_path, mode='wb') as local_file:
                local_file.write(data)
    except urllib.error.URLError as e:
        print('network error', e)


def get_image_url(image_id):
    try:
        random_t = random.randint(1, 10) / 10
        time.sleep(random_t)
        image_url = mly.image_thumbnail(image_id, 2048)
        return image_url
    except Exception as e:
        print('network error', e)


def download_image(image_id, dst_path):
    image_url = get_image_url(image_id)
    download_image_from_url(image_url, dst_path)
