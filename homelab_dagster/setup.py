from setuptools import find_packages, setup

setup(
    name="homelab_dagster",
    packages=find_packages(exclude=["homelab_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
