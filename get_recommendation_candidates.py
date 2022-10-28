"""Creates Beam pipeline code for your use case.

More into.
"""

import argparse
from datetime import datetime
import time
from typing import Any, Dict, Iterable, Type

import apache_beam as beam
from google.api_core import exceptions
from google.cloud import retail_v2 as retail


def build_pipeline(
        pipeline: Type[beam.Pipeline],
        args: Type[argparse.Namespace],
        prediction_ts: str) -> None:
    """Creates Beam pipeline for outputting predictions.

    Args:
        pipeline: Apache Beam pipeline.
        args: Namespace holding pipeline options for launching
            the Beam job.
        prediction_ts: A string containing a timestamp that identifies when
            predictions were generated.
    """
    table_schema = "id:INTEGER, prediction_ts:TIMESTAMP, " \
                         "message:STRING"    
    _ = (
        pipeline
        | "Get data" >> beam.io.ReadFromBigQuery(
        query="SELECT * FROM T",
        use_standard_sql=True)
        | "More steps" >> beam.io.ReadFromBigQuery(
        query="SELECT * FROM TT",
        use_standard_sql=True),
        | "Write valid results to BigQuery" >> beam.io.WriteToBigQuery(
        table="your_table",
        dataset=args.recsai_dataset,
        schema=table_schema,
        additional_bq_parameters={
            "ignoreUnknownValues": True
        },
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )
