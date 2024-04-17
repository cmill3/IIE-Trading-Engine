import unit_tests
from helpers.helper import *
from datetime import datetime
from unittest.mock import patch
import pytest
import warnings 
warnings.filterwarnings('ignore')

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

def create_sample_df_max_value():
    # Create a date range
    date_range = pd.date_range(start='2024-04-16 16:05', periods=8, freq='15min')
    # Convert datetime objects to UNIX timestamps (in seconds)
    timestamps = date_range.astype('int64') // 10**9

    data = {
        't': timestamps,  # timestamps every 15 minutes as integer UNIX timestamps
        'h': [150.0, 152.0, 155.0, 158.0, 157.0, 160.0, 159.0, 162.0],  # hypothetical high prices
    }
    df = pd.DataFrame(data)
    return df

@pytest.fixture
def sample_dataframe():
    return create_sample_df_max_value()

def test_get_derivative_max_value(sample_dataframe):
    # Mock the API call to return your sample dataframe
    with patch('helpers.helper.polygon_call_stocks', return_value=sample_dataframe):
        row = {"order_transaction_date": "2024-04-16T16:05:05.462Z", "option_symbol": "IWM240419C00196000"}
        max_value = get_derivative_max_value(row)
        print(max_value)
        assert max_value == 162.0, "Expected max value to be 162"



def create_sample_df_floor_pct():
    # Create a date range
    date_range = pd.date_range(start='2024-04-16 16:05', periods=8, freq='15min')
    # Convert datetime objects to UNIX timestamps (in seconds)
    timestamps = date_range.astype('int64') // 10**9

    data = {
        't': timestamps,  # timestamps every 15 minutes as integer UNIX timestamps
        'h': [150.0, 152.0, 155.0, 158.0, 157.0, 160.0, 159.0, 162.0],
        'l'  : [148.0, 150.0, 153.0, 156.0, 155.0, 158.0, 157.0, 160.0]
    }
    df = pd.DataFrame(data)
    print(df)
    return df

@pytest.fixture
def sample_dataframe():
    return create_sample_df_floor_pct()

@pytest.mark.parametrize("row, expected", [
    ({"order_transaction_date": "2024-04-16T16:05:05.462Z", "option_symbol": "IWM240419C00196000","trading_strategy":"CDBFC_1D","underlying_symbol":"IWM","underlying_purchase_price":"152.21"}, 162.0),
    ({"order_transaction_date": "2024-04-16T16:05:05.462Z", "option_symbol": "IWM240419C00196000","trading_strategy":"CDBFP_1D","underlying_symbol":"IWM","underlying_purchase_price":"152.21"}, 153.0),  # Out of trading hours
])
def test_calculate_floor_pct(sample_dataframe,row,expected):
    # Mock the API call to return your sample dataframe
    with patch('helpers.helper.polygon_call_stocks', return_value=sample_dataframe):
        value = calculate_floor_pct(row)
        assert value == expected, "Expected max value to be 162"

