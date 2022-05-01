import os

import apache_beam as beam
import pandas as pd
from google.cloud import storage


class Load(beam.DoFn):
    def __init__(self, pipeline_root='pipeline-root', data_dir='data'):
        self.pipeline_root = pipeline_root
        self.data_dir = data_dir
        os.makedirs(os.path.join(pipeline_root, data_dir), exist_ok=True)

    def process(self, element):
        storage_client = storage.Client.create_anonymous_client()
        self.bucket = storage_client.bucket(element)
        for blob in self.bucket.list_blobs():
            file_name = os.path.basename(blob.name)
            local_file = os.path.join(
                self.pipeline_root, self.data_dir, file_name)
            blob.download_to_filename(local_file)
            yield local_file


class Save(beam.DoFn):
    def __init__(self, pipeline_root='pipeline-root', output_dir='output'):
        self.pipeline_root = pipeline_root
        self.output_dir = output_dir
        os.makedirs(os.path.join(pipeline_root, output_dir), exist_ok=True)

    def process(self, element):
        basename = os.path.basename(element)
        craft, planet, _, _ = basename.split('.')[0].split('_')
        output_file = f'{craft}_{planet}.csv'
        output_file = os.path.join(
            self.pipeline_root, self.output_dir, output_file)

        df = pd.read_csv(element)
        df.to_csv(output_file, mode='a', index=False,
                  header=not os.path.exists(output_file))
        return output_file
