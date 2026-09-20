from setuptools import find_packages, setup

setup(
    name="firewatch-streaming",
    version="1.0",
    packages=find_packages(),
    include_package_data=True,
    package_data={"utils": [".env"]},
    install_requires=[
        "apache-beam[gcp]",
        "fastavro",
        "python-dotenv",
    ],
)