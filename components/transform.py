import os
from datetime import datetime

import apache_beam as beam
import pandas as pd


class ToLowercaseColumns(beam.DoFn):
    def process(self, element):
        df = pd.read_csv(element)
        df.columns = self._to_lowercase(df.columns)
        df.to_csv(element, index=False)
        yield element

    def _to_lowercase(self, items):
        return [item.lower() for item in items]


class ExtractDateTime(beam.DoFn):
    def process(self, element):
        basename = os.path.basename(element)
        _, _, date, time = basename.split('.')[0].split('_')
        df = pd.read_csv(element)
        timestamp = datetime.strptime(f'{date} {time}', '%Y%m%d %H%M%S')
        df['timestamp'] = timestamp
        df.to_csv(element, index=False)
        yield element


class ParseID(beam.DoFn):
    def process(self, element):
        df = pd.read_csv(element)
        df['id'] = df['id'].apply(self._extract_id)
        df.to_csv(element, index=False)
        yield element

    def _extract_id(self, id):
        return id.split('-')[2]


class ToNumeric(beam.DoFn):
    def __init__(self, column_name):
        self.column_name = column_name

    def process(self, element):
        df = pd.read_csv(element)
        df[self.column_name] = pd.to_numeric(
            df[self.column_name], errors='coerce')
        df.to_csv(element, index=False)
        yield element


class Magnitude(beam.DoFn):
    def process(self, element):
        df = pd.read_csv(element)
        df['magnitute'] = df['size'].apply(self._map_size)
        df.to_csv(element, index=False)
        yield element

    def _map_size(self, size):
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


class DropColumn(beam.DoFn):
    def __init__(self, column_name):
        self.column_name = column_name

    def process(self, element):
        df = pd.read_csv(element)
        df.drop(self.column_name, axis=1, inplace=True)
        df.to_csv(element, index=False)
        yield element
