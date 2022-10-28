"""Runs the serving pipeline for your model."""

# Disabling the following pylint warnings as Vertex AI pipeline components
# require imports to be inside of functions:
# pylint: disable=redefined-outer-name
# pylint: disable=reimported
# pylint: disable=import-outside-toplevel
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional, Type
import os
import re
import sys
import yaml

import google.auth
from google.cloud import storage
import google.cloud.aiplatform as aip


def parse_arguments(argv: List[str]) -> Type[argparse.Namespace]:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config_file",
        help="Local or GCS path to YAML config file for pipeline parameters.",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--template_file",
        help="Local or GCS path to JSON pipeline template file.",
        type=str,
        required=True,
    )
    arguments, _ = parser.parse_known_args(argv)
    return arguments


def get_config(
    path: str,
    credentials: Optional[google.auth.credentials.Credentials] = None,
) -> Dict[str, Any]:
    """Loads configs from a YAML file to a dict.

    Args:
        path: Path to the config YAML either locally or on GCS.
        credentials: Optional custom credentials to use to call Google Cloud
            APIs. Default is `None` which infers credentials dynamically.

    Returns:
        The configs in a dict.
    """
    if path.startswith("gs://"):
        client = storage.Client(credentials=credentials)
        match = re.match(r"gs://([^/]+)/(.+)", path)
        bucket_name = match.group(1)
        blob_name = match.group(2)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        config_as_string = blob.download_as_string()
        config = yaml.safe_load(config_as_string)
    else:
        with open(path, "r", encoding="utf-8") as infile:
            config = yaml.safe_load(infile)
    return config


def validate_config(config: Type[yaml.safe_load]) -> None:
    """Validates config file.

    Args:
        config: The configs in a dict.
    """
    args = [
        "data_project_id",
        "dataflow_region",
        "machine_type",
        "max_num_workers",
        "bucket",
        "pipeline_job_name",
        "labels",
        "tar_path",    
    ]
    for arg in args:
        if arg not in config:
            raise ValueError(f"`{arg}` must be defined in the config file.")

    if not config["bucket"].startswith("gs://"):
        raise ValueError("Expected `bucket` to start with `gs://`. Got `" +
                         config["bucket"] + "`.")


def run_pipeline(
    template_path: str,
    config_path: str,
    credentials: Optional[google.auth.credentials.Credentials] = None,
) -> None:
    """Executes Vertex AI Pipeline.

    This function parses arguments from a YAML config file for ease-of-use for
    the end user. The pipeline itself does not know about the config file.

    Args:
        template_path: GCS path to the JSON pipeline template.
        config_path: GCS path to the YAML config file.
        credentials: Optional custom credentials to use to call Google Cloud
            APIs. Default is `None` which infers credentials dynamically.
    """
    config = get_config(config_path, credentials)
    validate_config(config)

    bucket = config["bucket"]
    service_account = config.get("service_account")
    pipeline_root = os.path.join(bucket, "pipeline_root")
    component_args = [
        # `vertex_pipelines` is a dummy parameter replacing args[0] when
        #  calling your python scripts from the pipeline
        "vertex_pipelines",
        "--data_project_id", config["data_project_id"],
        "--recsai_project_id", config["recsai_project_id"],
        "--region", config["dataflow_region"],
        "--machine_type", config["machine_type"],
        "--max_num_workers", str(config["max_num_workers"]),
    ]
    if service_account:
        component_args += ["--service_account", service_account]
    if config.get("test"):
        component_args.append("--test" )

    aip.init(
        project=config["data_project_id"],
        location=config["dataflow_region"],
        credentials=credentials,
        staging_bucket=bucket)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    job = aip.PipelineJob(
        enable_caching=False,
        display_name=config["pipeline_job_name"] + f"-{timestamp}",
        template_path=template_path,
        pipeline_root=pipeline_root,
        labels=config["labels"],
        parameter_values={
            "tar_path": os.path.join(bucket, config["tar_path"]),
            "component_args": component_args,
        },
    )
    job.run(service_account=service_account)


if __name__ == "__main__":
    args = parse_arguments(sys.argv)
    run_pipeline(
        template_path=args.template_file,
        config_path=args.config_file,
    )
