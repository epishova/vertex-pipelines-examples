"""Config for your package."""

from setuptools import find_packages
from setuptools import setup

REQUIRED_PACKAGES = [
    "google-cloud-bigquery==2.31.0",
    "google-cloud-bigquery-storage==2.10.1",
    "google-cloud-retail==1.2.1",
    "pytest==6.2.5",
    "pyyaml==6.0",
]

setup(
    name="your_package_name",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=REQUIRED_PACKAGES,
    description="What your package is doing."
)
