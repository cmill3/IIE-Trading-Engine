from helpers.helper import *
from datetime import datetime

def test_convert_timestamp_est():
    timestamp = 1713283505.446
    est_time = convert_timestamp_est(timestamp)
    est_str = est_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    assert est_str == "2024-04-16T12:05:05.446000Z", "Time should be 2024-04-16 12:05:05"
    assert type(est_time) == datetime, "Time should be a datetime object"

def test_datetime_to_timestamp_UTC():
    datetime_str = "2024-04-16T16:05:05.446Z"
    timestamp = datetime_to_timestamp_UTC(datetime_str)

    assert timestamp == 1713283505.446, "Timestamp should be 1713283505.446"
    assert type(timestamp) == float, "Timestamp should be an integer"

def test_pull_model_config():
    config = pull_model_config("CDBFC_1D")
    assert config is not None, "Config should not be None"
    assert type(config) == dict, "Config should be a dictionary"
    assert config["strategy"] == "CDBFC_1D", "Strategy should be CDBFC_1D"
