# %%
import apache_beam as beam

from components.io import Load, Save
from components.transform import (DropColumn, ExtractDateTime, Magnitude,
                                  ParseID, ToLowercaseColumns, ToNumeric)


def run_pipeline():
    with beam.Pipeline() as pipeline:
        p = (
            pipeline | beam.Create(["de-assignment-data-bucket"])
            | 'LoadFiles' >> beam.ParDo(Load())
            | 'ToLowercaseColumns' >> beam.ParDo(ToLowercaseColumns())
            | 'ExtractDateTime' >> beam.ParDo(ExtractDateTime())
            | 'ParseID' >> beam.ParDo(ParseID())
            | 'ToNumeric' >> beam.ParDo(ToNumeric('size'))
            | 'SetMagnitude' >> beam.ParDo(Magnitude())
            | 'DropSizeColumn' >> beam.ParDo(DropColumn('size'))
            | 'CombineFiles' >> beam.ParDo(Save())
        )


# %%

run_pipeline()
