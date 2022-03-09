import os, sys
import shutil

import absl
from zipfile import ZipFile
from tfx.components import CsvExampleGen

from census_consumer_complaint_exception.exception import CensusConsumerException
from tfx.types import standard_artifacts
from tfx.types.component_spec import ChannelParameter, ExecutionParameter
from typing import Text, List, Dict, Any
from tfx.types import ComponentSpec
from tfx.components.base.base_executor import BaseExecutor
from tfx.components.base.base_driver import BaseDriver
# from tfx.dsl.components.base.base_component import BaseComponent
from tfx.components.base.base_component import BaseComponent
from tfx import types
from tfx.types import artifact_utils, channel_utils
from tfx.orchestration import data_types
from census_consumer_complaint_utils.utils import download_dataset
from tfx.components import CsvExampleGen
from tfx.components.base import executor_spec

PARAMETER_NAME_KEY = "name"
OUTPUT_CSV_KEY = "csv"
INPUT_BASE_KEY = "input_base"
TEMP_DIR = os.path.join("TEMP")
ZIP_FILE_READ_MODE = "r"


class ZipCsvIngestSpec(ComponentSpec):
    PARAMETERS = {
        PARAMETER_NAME_KEY: ExecutionParameter(type=Text),
        INPUT_BASE_KEY:
            ExecutionParameter(type=str),
    }

    INPUTS = {

    }

    OUTPUTS = {
        OUTPUT_CSV_KEY: ChannelParameter(type=standard_artifacts.Examples)
    }


class ZipCSVIngestExecutor(BaseExecutor):
    def Do(self,
           input_dict: Dict[Text, List[types.Artifact]],
           output_dict: Dict[Text, List[types.Artifact]],
           exec_properties: Dict[Text, Any]
           ) -> None:
        try:
            self._log_startup(input_dict, output_dict, exec_properties)
            input_base_uri = exec_properties[INPUT_BASE_KEY]
            download_dir = artifact_utils.get_single_uri(output_dict[OUTPUT_CSV_KEY])
            downloaded_file_uri = download_dataset(input_base_uri, TEMP_DIR)
            with ZipFile(downloaded_file_uri, ZIP_FILE_READ_MODE) as zip_file:
                os.makedirs(download_dir, exist_ok=True)
                zip_file.extractall(download_dir)
            if os.path.exists(TEMP_DIR):
                shutil.rmtree(TEMP_DIR)
        except Exception as e:
            raise (CensusConsumerException(e, sys)) from e


class ZipCSVIngestDriver(BaseDriver):

    def resolve_input_artifacts(self, input_channels: Dict[Text, types.Channel],
                                exec_properties: Dict[Text, Any],
                                driver_args: data_types.DriverArgs,
                                pipeline_info: data_types.PipelineInfo
                                ) -> Dict[Text, List[types.Artifact]]:
        try:
            del driver_args
            del pipeline_info
            input_dict = channel_utils.unwrap_channel_dict(input_channels)
            for input_list in input_dict.values():
                for single_input in input_list:
                    self._metadata_handler.publish_artifacts([single_input])
                    absl.logging.debug(f"Registered input: {single_input}")
                    absl.logging.debug(f"single_input.mlmd_artifact {single_input.mlmd_artifact}")
            return input_dict
        except Exception as e:
            raise (CensusConsumerException(e, sys)) from e


class ZipCsvExtractorGen(BaseComponent):
    SPEC_CLASS = ZipCsvIngestSpec
    EXECUTOR_SPEC = executor_spec.ExecutorClassSpec(ZipCSVIngestExecutor)
    DRIVER_CLASS = ZipCSVIngestDriver

    def __init__(self, input_base, output_data=None, name=None):
        try:
            if not output_data:
                examples_artifact = standard_artifacts.Examples()
                output_data = channel_utils.as_channel([examples_artifact])

            spec = ZipCsvIngestSpec(input_base=input_base,
                                    csv=output_data,
                                    name=name
                                    )
            super(ZipCsvExtractorGen, self).__init__(spec=spec)
        except Exception as e:
            raise (CensusConsumerException(e, sys)) from e
