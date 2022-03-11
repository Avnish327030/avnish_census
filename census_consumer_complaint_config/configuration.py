import os
import datetime

ROOT_DIR = os.path.join(os.getcwd(),"census_consumer_complaint_data")
PIPELINE_NAME = "census_consumer_complaint"
PIPELINE_ARTIFACT = "artifact"
META_DATA_SQLITE_FILE_DIR = "meta_data"
SQLITE_FILE_NAME = "meta_data.db"
LOG_DIR = 'logs'
SERVING_MODEL_DIR = "saved_models"
DAYS = 1  # scheduled interval day
SCHEDULED_INTERVAL = datetime.timedelta(days=DAYS)
START_DATE = datetime.datetime(2022, 3, 5)


class CensusConsumerConfiguration:

    def __init__(self):
        self.root_dir = os.path.join(ROOT_DIR)
        self.pipeline_name = PIPELINE_NAME
        self.pipeline_root = os.path.join(ROOT_DIR, PIPELINE_NAME, PIPELINE_ARTIFACT)
        self.metadata_path = os.path.join(ROOT_DIR, META_DATA_SQLITE_FILE_DIR, SQLITE_FILE_NAME)
        self.log_dir = os.path.join(ROOT_DIR, LOG_DIR)
        self.serving_model_dir = os.path.join(ROOT_DIR, SERVING_MODEL_DIR)
        self.scheduled_interval = SCHEDULED_INTERVAL
        self.start_date = START_DATE
