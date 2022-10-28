# vertex-pipelines-examples
Examples of building Vertex AI Pipelines

## Deploy your existing python code on Vertex AI Pipelines in a fast way

Consider the following setup:

* You have python scripts to perform ML tasks that you run on your local computer. Your working directory contains `config.yaml`, `requirements.txt`, etc. and looks like this directory. 
* You store your python scripts in the `serving` folder with the main script called `run_predict.py`

You want to run all those python scripts on Vertex AI Pipelines. You can pack your existing code in a python package, store it on GCS, and call it using a python component:

* You create a new directory `vertex_pipelines` to store all related code
* You add `setup.py` to build `vertex-pipelines-examples` python package:

** Make sure you are in the correct folder.

```
$ cd /vertex-pipelines-examples
python setup.py sdist
```

** Copy the resulting your_package_name.tar.gz to the GCP bucket.

```
gsutil cp ./dist/your_package_name.tar.gz gs://your_backet/your_package_name.tar.gz
```

** Copy your existing configuration file into vertex_pipelines directory.

```
cp config.yaml vertex_pipelines/config/config.yaml
```

** Add new configurations to `vertex_pipelines/config/config.yaml`. Below, `bucket/tar_path` is the location of `your_package_name.tar.gz` from the step above.

```
bucket: 'gs://your_backet'
tar_path: 'pipelines/your_package_name/your_package_name.tar.gz'
pipeline_job_name: `your_package_name-serving'
labels:
    use-case: your_label
```

** Run the serving pipeline on Vertex AI Pipelines as shown in compilej_and_run_pipeline.py.