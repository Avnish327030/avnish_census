import os
import sys
from census_consumer_complaint_exception.exception import CensusConsumerException
from tfx.orchestration.metadata import sqlite_metadata_connection_config
from tfx.orchestration.experimental.interactive.interactive_context import InteractiveContext
from tfx.orchestration.local.local_dag_runner import LocalDagRunner
from tfx.orchestration.pipeline import Pipeline
from collections import namedtuple

PIPELINE_NAME = "census_consumer_pipeline"
META_DATA_DIR = "census_consumer_meta_data"
META_DATA_DB_NAME = "census_consumer_meta_data.db"
LOCAL_DAG_RUNNER = namedtuple("LocalDagRunner", ["local_dag_runner_obj", "pipeline"])


class CensusConsumerPipelineConfiguration:
    """
    CensusConsumerPipelineConfiguration
    """

    def __init__(self, *args, **kwargs):
        try:
            self.pipeline_name = PIPELINE_NAME
            self.pipeline_root = os.path.join(PIPELINE_NAME)
            self.meta_data_db_path = os.path.join(META_DATA_DIR, META_DATA_DB_NAME)
        except Exception as e:
            raise (CensusConsumerException(e, sys)) from e


class CensusConsumerInteractiveContext(CensusConsumerPipelineConfiguration):

    def __init__(self, *args, **kwargs):
        try:
            super(CensusConsumerInteractiveContext, self).__init__(*args, **kwargs)
            self.interactive_context = None
        except Exception as e:
            raise (CensusConsumerException(e, sys)) from e

    def get_interactive_context(self):
        try:

            if self.interactive_context is None:
                self.interactive_context = InteractiveContext(
                    pipeline_name=self.pipeline_name,
                    pipeline_root=self.pipeline_root,
                    metadata_connection_config=sqlite_metadata_connection_config(self.meta_data_db_path)
                )
            return self.interactive_context
        except Exception as e:
            raise (CensusConsumerException(e, sys)) from e

