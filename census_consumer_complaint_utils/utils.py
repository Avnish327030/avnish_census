import sys, os
import tensorflow as tf
import shutil, wget
from census_consumer_complaint_exception.exception import CensusConsumerException
from urllib import request


# defining function to convert value to appropriate data type which tf.Example accepts
def _bytes_feature(value):
    """
    created by: Avnish Yadav
    created on: 27/02/2022
    version: 1.0
    """
    try:
        value = bytes(value, encoding="utf-8")
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))
    except Exception as e:
        raise Exception(CensusConsumerException(e, sys)) from e


def _float_feature(value):
    try:
        """
        created by: Avnish Yadav
        created on: 27/02/2022
        version: 1.0
        """
        return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))
    except Exception as e:
        raise Exception(CensusConsumerException(e, sys)) from e


def _int64_feature(value):
    """
    created by: Avnish Yadav
    created on: 27/02/2022
    version: 1.0
    """
    try:
        return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))
    except Exception as e:
        raise Exception(CensusConsumerException(e, sys)) from e


def download_dataset(uri: str, download_dir: str) -> str:
    """
    created by: Avnish Yadav
    created on: 27/02/2022
    version: 1.0
    Download the dataset from the uri and save it in the download_dir
    ============================================================================
    uri: The uri of the dataset
    download_dir: The directory where the dataset will be downloaded
    """
    try:

        os.makedirs(download_dir, exist_ok=True)
        file_name = os.path.basename(uri)
        file_path = os.path.join(download_dir, file_name)
        request.urlretrieve(uri, file_path)
        return file_path
    except Exception as e:
        raise Exception(CensusConsumerException(e, sys)) from e
