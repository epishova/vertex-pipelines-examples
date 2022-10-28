"""Creates serving pipeline code for your use case.

More details here.
"""

import argparse
import datetime
import logging
import os
import sys
from typing import List, Type
import yaml

import apache_beam as beam
from apache_beam.options import pipeline_options
from google.api_core import exceptions
from google.cloud import bigquery

from serving import get_recommendation_candidates
from serving import query


def parse_arguments(argv: List[str]) -> Type[argparse.Namespace]:
    """Parses command line arguments."""
    config_parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False)
    config_parser.add_argument(
        "--config_file",
        help="Local path to YAML config file.",
        default="config.yaml",
        type=str)
    args, _ = config_parser.parse_known_args(argv)
    parser = argparse.ArgumentParser(
        parents=[config_parser],
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    if os.path.exists(args.config_file):
        # pylint: disable=unspecified-encoding
        with open(args.config_file, "r") as yml_file:
            config = yaml.safe_load(yml_file)
        parser.set_defaults(**config)
    else:
        config = {}
        logging.info(
            f"""Config file {args.config_file} not found. Continuing anyways,
            but some functions may fail due to unspecified defaults.""")

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    parser.add_argument(
        "--cloud",
        help="Run preprocessing on the cloud. Default False.",
        action="store_true",
        default=False)
    parser.add_argument(
        "--job_name",
        help="Dataflow job name.",
        type=str,
        default=f"prediction-serving-{timestamp}")
    parser.add_argument(
        "--job_dir",
        help="""GCS bucket to stage code and write temporary outputs for cloud
            runs.""",
        type=str,
        default=config.get("job_dir"))
    parser.add_argument(
        "--region",
        help="Dataflow region.",
        type=str,
        default=config.get("dataflow_region", "us-east4"))
    parser.add_argument(
        "--machine_type",
        help="Set machine type for Dataflow worker machines.",
        type=str,
        default=config.get("machine_type", "n1-standard-8"))
    parser.add_argument(
        "--data_project_id",
        help="GCP project ID that contains BigQuery source data.",
        type=str,
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    # Ohter arguments.

    args, _ = parser.parse_known_args(args=argv[1:])

    # Validate CLI values.
    if args.data_project_id is None:
        raise ValueError("`data_project_id` is required.")

    return args


def get_pipeline_options(
        args: Type[argparse.Namespace]
) -> Type[pipeline_options.PipelineOptions]:
    """Returns pipeline options."""
    if not args.recsai_project_id:
        raise ValueError("Project name must be specified.")
    if "gs://" not in args.job_dir:
        raise ValueError(
            "A job directory in Cloud Storage must be specified.")
    options = {
        "project": args.recsai_project_id,
        "temp_location": os.path.join(args.job_dir, "tmp"),
    }
    if args.cloud:
        if not args.job_dir:
            raise ValueError("Job directory must be specified for Dataflow.")
        options.update({
            "job_name": args.job_name,
            "setup_file": args.setup_file,
            "staging_location": os.path.join(args.job_dir, "staging"),
            "region": args.region,
            "use_public_ips": False,
            "subnetwork": args.subnetwork,
            "max_num_workers": args.max_num_workers
        })
        options = pipeline_options.PipelineOptions(flags=[], **options)
        options.view_as(
            pipeline_options.WorkerOptions).machine_type = args.machine_type
    else:
        options = pipeline_options.PipelineOptions(flags=[], **options)
    return options


def main(argv: List[str]) -> None:
    """Generates ML predictions.

    Args:
        argv: Arguments to set up the process.
    """
    args = parse_arguments(argv)
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
    prediction_ts = datetime.datetime.now()
    bq_client = bigquery.Client(project=args.recsai_project_id)

    query_generator = query.QueryGenerator(args, prediction_ts)
    # Create predictions using Beam.
    options = get_pipeline_options(args)
    runner = "DataflowRunner" if args.cloud else "DirectRunner"
    with beam.Pipeline(runner, options=options) as pipeline:
        get_recommendation_candidates.build_pipeline(
            pipeline,
            args,
            prediction_ts)

    # Post-filter predictions in BigQuery.
    request = query_generator.get_predictions_view()
    query_job = bq_client.query(request)
    query_job.result()


if __name__ == "__main__":
    main(sys.argv)
