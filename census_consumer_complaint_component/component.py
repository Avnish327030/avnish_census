import sys

from census_consumer_complaint_component.data_ingestion import get_data_ingestion_components
from census_consumer_complaint_component.data_validation import get_data_validation_components
from census_consumer_complaint_exception.exception import CensusConsumerException


def get_census_consumer_complaint_pipeline_component():
    try:
        pipeline_component = []
        data_ingestion = get_data_ingestion_components()

        pipeline_component.append(data_ingestion.zip_csv_extractor_gen)
        pipeline_component.append(data_ingestion.csv_example_gen)
        data_validation = get_data_validation_components(csv_example_gen=data_ingestion.csv_example_gen)
        pipeline_component.append(data_validation.statistic_gen)
        pipeline_component.append(data_validation.schema_gen)

        return pipeline_component

    except Exception as e:
        raise (CensusConsumerException(e, sys)) from e
