# Copyright 2022 Google LLC. This software is provided as-is, without warranty
# or representation for any use or purpose. Your use of it is subject to your
# agreement with Google.
"""Builds the serving pipeline for the Inspiration Items model."""

# Disabling the following pylint warnings as Vertex AI pipeline components
# require imports to be inside of functions:
# pylint: disable=redefined-outer-name
# pylint: disable=reimported
# pylint: disable=import-outside-toplevel
from typing import List

from kfp.v2 import compiler
from kfp.v2 import dsl


@dsl.component(packages_to_install=[
    "apache-beam[gcp]==2.35.0",
    "google-cloud-bigquery==2.31.0",
    "google-cloud-bigquery-storage==2.10.1",
    "google-cloud-retail==1.2.1",
    "tensorflow==2.7.0"])
def get_recommendations_op(
        gcs_tar_path: str,
        python_module_args: List[str]) -> None:
    """Defines the Python function based component.

    A Python function based component is used to install and call custom Python
    packages.

    Args:
        gcs_tar_path: GCS location of the model serving Python package.
        python_module_args: A list of input arguments to the model serving
        module.
    """
    import os
    import subprocess
    import tarfile
    import tensorflow as tf

    # Copy the serving package.
    local_tar_path = "inspiration.tar.gz"
    tf.io.gfile.copy(gcs_tar_path, local_tar_path, overwrite=True)
    with tarfile.open(local_tar_path) as tar:
        tar.extractall()
    try:
        local_tarfile = tf.io.gfile.glob("inspiration-*")[0]
    except IndexError as no_import_pkg:
        print(f"Package {local_tar_path} could not be unpacked.")
        raise no_import_pkg
    os.rename(local_tarfile, "inspiration")

    # Install the serving package.
    command = ["pip", "install", local_tar_path]
    subprocess.run(command, check=True)
    os.chdir("inspiration")

    # Run the serving script.
    from serving import run_predict
    run_predict.main(python_module_args)


@dsl.pipeline(
    name="inspiration-serving",
    description="Serving pipeline for Inspiration use case.",
)
def pipeline(
        tar_path: str,
        component_args: List[str]) -> None:
    """Defines Vertex AI Pipeline that runs the model serving.

    Args:
        tar_path: GCS location of the model serving Python package.
        component_args: A list of input arguments to the model serving module.
    """
    get_recommendations_op(
        gcs_tar_path=tar_path,
        python_module_args=component_args,
    )


def compile_pipeline(template_path: str) -> None:
    """Compiles Vertex AI Pipeline.

    Args:
        template_path: Path including the file name to store the compiled
            pipeline.
    """
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=template_path,
    )
