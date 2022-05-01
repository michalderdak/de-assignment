# %%

import pandas as pd
from datetime import datetime
from google.cloud import storage
import os

BUCKET_NAME = 'de-assignment-data-bucket'
storage_client = storage.Client.create_anonymous_client()

pipeline_root = '/Users/michalderdak/personal/de-assignment/pipeline-root'
os.makedirs(pipeline_root, exist_ok=True)


def download_all_data():
    bucket = storage_client.bucket(BUCKET_NAME)
    for blob in bucket.list_blobs():
        file_name = os.path.basename(blob.name)
        local_file_name = os.path.join(pipeline_root, file_name)
        blob.download_to_filename(local_file_name)
        yield file_name


it = iter(download_all_data())

# %%
file_name = next(it)


# %%
def lowercase_columns(file_name):
    file_path = os.path.join(pipeline_root, file_name)
    df = pd.read_csv(file_path)
    df.columns = [col.lower() for col in df.columns]
    df.to_csv(file_path, index=False)
    return file_name


file_name = lowercase_columns(file_name)


# %%
def extract_date_time(file_name):
    _, _, date, time = file_name.split('.')[0].split('_')
    file_path = os.path.join(pipeline_root, file_name)
    df = pd.read_csv(file_path)
    timestamp = datetime.strptime(f'{date} {time}', '%Y%m%d %H%M%S')
    df['timestamp'] = timestamp
    df.to_csv(file_path, index=False)
    return file_name


file_name = extract_date_time(file_name)


# %%
def parse_id(file_name):
    file_path = os.path.join(pipeline_root, file_name)
    df = pd.read_csv(file_path)
    df['id'] = df['id'].apply(lambda x: x.split('-')[2])
    df.to_csv(file_path, index=False)
    return file_name


file_name = parse_id(file_name)


# %%
def filter_size(file_name):
    file_path = os.path.join(pipeline_root, file_name)
    df = pd.read_csv(file_path)
    df['size'] = pd.to_numeric(df['size'], errors='coerce')
    df.to_csv(file_path, index=False)
    return file_name


file_name = filter_size(file_name)


# %%
def _map_size(size):
    if 500 <= size <= 1000:
        return 'massive'
    if 100 <= size < 500:
        return 'big'
    if 50 <= size < 100:
        return 'medium'
    if 10 <= size < 50:
        return 'small'
    if 1 <= size < 10:
        return 'tiny'
    return 'unknown'


def magnitute_size(file_name):
    file_path = os.path.join(pipeline_root, file_name)
    df = pd.read_csv(file_path)
    df['magnitute'] = df['size'].apply(_map_size)
    df.to_csv(file_path, index=False)
    return file_name


file_name = magnitute_size(file_name)


# %%
def drop_column(file_name, column_name):
    file_path = os.path.join(pipeline_root, file_name)
    df = pd.read_csv(file_path)
    df.drop(column_name, axis=1, inplace=True)
    df.to_csv(file_path, index=False)
    return file_name


file_name = drop_column(file_name, 'size')


# %%
def save_to_single_file(file_name):
    file_path = os.path.join(pipeline_root, file_name)
    df = pd.read_csv(file_path)
    craft, planet, _, _ = file_name.split('.')[0].split('_')
    output_file = f'{craft}_{planet}.csv'
    output_file = os.path.join(pipeline_root, output_file)
    df.to_csv(output_file, mode='a', index=False,
              header=not os.path.exists(output_file))
    return file_name


file_name = save_to_single_file(file_name)

# %%
