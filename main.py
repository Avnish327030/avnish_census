import re
from census_consumer_complaint_utils.utils import download_dataset
from census_consumer_complaint_custom_component.custom_component import ZipCsvExtractorGen
from census_consumer_complaint_config.configuration import CensusConsumerConfiguration
from tfx.components import CsvExampleGen, StatisticsGen
from tfx.orchestration.pipeline import Pipeline
from tfx.orchestration.local.local_dag_runner import LocalDagRunner
from tfx.orchestration.metadata import sqlite_metadata_connection_config
import os
from tfx.components.base.base_component import BaseComponent
from urllib import request
from census_consumer_complaint_orchestrator.apache_beam_orchestrator import run_apache_dag_pipeline
from census_consumer_complaint_orchestrator.local_orchestrator import run_local_dag_runner_pipeline

def test():
    url = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
    print(os.path.basename(url))
    request.urlretrieve(url, "complaints.zip")


if __name__ == "__main__":

    #run_apache_dag_pipeline()
    run_local_dag_runner_pipeline()



