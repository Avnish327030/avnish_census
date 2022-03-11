import sys
import os
from census_consumer_complaint_exception.exception import CensusConsumerException

from tfx.components import SchemaGen, StatisticsGen, ExampleValidator

from census_consumer_complaint_custom_component.component import RemoteZipFileBasedExampleGen

from collections import namedtuple

DataValidation = namedtuple("DataValidation", ["statistic_gen", "schema_gen"])


def get_data_validation_components(zip_example_gen: RemoteZipFileBasedExampleGen) -> DataValidation:
    """
    :param zip_example_gen:
    :param self:
    :return: List of tfx component
    """
    try:
        data_validation_components = []
        statistic_gen = StatisticsGen(
            examples=zip_example_gen.outputs['examples']
        )

        schema_gen = SchemaGen(
            statistics=statistic_gen.outputs['statistics']
        )

        return DataValidation(statistic_gen=statistic_gen,
                              schema_gen=schema_gen)

    except Exception as e:
        raise CensusConsumerException(e, sys) from e
