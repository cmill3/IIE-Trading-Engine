import unit_tests
from helpers.dynamo_helper import get_trading_balance

def test_get_trading_balance():
    # Setup
    # Here you might want to set up any necessary objects or state for the test.
    # For example, if get_trading_balance() requires a database connection, you might set that up here.
    trading_strategy = "CDVOLBF"
    env = "PROD_VAL"
    # Exercise
    result = get_trading_balance(trading_strategy,env)

    # Verify
    # Here you should check that the result is what you expect.
    # Without knowing what get_trading_balance() does, it's hard to give a specific example.
    # But here's a generic one:
    assert result is not None, "Result should not be None"
    assert type(result) == float, "Result should be a float"

def test_get_trading_balance_no_env():
    # Setup
    trading_strategy = "CDVOLBF"
    env = None
    # Exercise
    with unit_tests.raises(ValueError):
        get_trading_balance(trading_strategy,env)



