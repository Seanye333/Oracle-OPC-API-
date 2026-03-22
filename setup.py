from setuptools import setup, find_packages

setup(
    name="oracle-opc-pipeline",
    version="1.0.0",
    description="Oracle OPC P6 data pipeline: extract, transform, load to Oracle DB and MinIO",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "requests>=2.31.0",
        "requests-oauthlib>=1.3.1",
        "python-oracledb>=2.1.2",
        "minio>=7.2.7",
        "pandas>=2.1.4",
        "pyarrow>=14.0.2",
        "numpy>=1.26.3",
        "PyYAML>=6.0.1",
        "python-dotenv>=1.0.0",
        "tenacity>=8.2.3",
        "python-json-logger>=2.0.7",
        "python-dateutil>=2.8.2",
        "pytz>=2023.3",
    ],
)
