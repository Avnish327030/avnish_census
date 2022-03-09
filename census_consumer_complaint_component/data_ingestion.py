import sys
import os
from census_consumer_complaint_exception.exception import CensusConsumerException

from census_consumer_complaint_custom_component.custom_component import ZipCsvExtractorGen
from tfx.components import CsvExampleGen
from tfx.components.base.base_component import BaseComponent
from typing import List
from collections import namedtuple

INPUT_DATASET_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"

ZIP_CSV_EXTRACTOR_GEN_NAME = "ZIP_CSV_EXTRACTOR_GEN"

DataIngestion = namedtuple("DataIngestion", ["zip_csv_extractor_gen", "csv_example_gen"])


def get_data_ingestion_components(url=INPUT_DATASET_URL) -> DataIngestion:
    """
    :param url:
    :param self:
    :return: List of tfx component
    """
    try:

        input_config = {

        }
        output_config = {

        }
        zip_csv_extractor_gen = ZipCsvExtractorGen(input_base=url,
                                                   name=ZIP_CSV_EXTRACTOR_GEN_NAME
                                                   )

        csv_example_gen = CsvExampleGen(input_base=zip_csv_extractor_gen.outputs['csv'].get()[0].uri
                                        )

        return DataIngestion(zip_csv_extractor_gen=zip_csv_extractor_gen,
                             csv_example_gen=csv_example_gen
                             )

    except Exception as e:
        raise CensusConsumerException(e, sys) from e
