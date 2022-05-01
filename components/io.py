import os

import apache_beam as beam
import pandas as pd
from google.cloud import storage


class Load(beam.DoFn):
    def __init__(self, pipeline_root='pipeline-root', data_dir='data'):
        self.pipeline_root = pipeline_root
        self.data_dir = data_dir

    def process(self, element):
        os.makedirs(os.path.join(self.pipeline_root,
                    self.data_dir), exist_ok=True)
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

    def _get_output_file(self, file_name):
        basename = os.path.basename(file_name)
        craft, planet, _, _ = basename.split('.')[0].split('_')
        output_file = f'{craft}_{planet}.csv'
        output_file = os.path.join(
            self.pipeline_root, self.output_dir, output_file)
        return output_file

    def process(self, element):
        os.makedirs(os.path.join(self.pipeline_root,
                    self.output_dir), exist_ok=True)
        output_file = self._get_output_file(element)
        df = pd.read_csv(element)
        df.to_csv(output_file, mode='a', index=False,
                  header=not os.path.exists(output_file))
        return output_file
