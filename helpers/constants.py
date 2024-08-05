CALL_STRATEGIES = ["CDBFC","CDBFC_1D","CDLOSEC_1D","CDGAINC_1D"]
PUT_STRATEGIES = ["CDBFP","CDBFP_1D","CDGAINP_1D","CDLOSEP_1D"]
ONED_STRATEGIES = ["CDBFC_1D","CDBFP_1D","CDGAINC_1D","CDGAINP_1D","CDLOSEP_1D","CDLOSEC_1D"]
CDVOL_STRATEGIES = ["CDBFC","CDBFP","CDBFC_1D","CDBFP_1D"]
TREND_STRATEGIES = ["CDGAINC_1D","CDGAINP_1D","CDLOSEP_1D","CDLOSEC_1D"]

ALGORITHM_CONFIG = {
    "CDBFC_1D": {
        "target_label": "one_max_vol",
        "spread_start": 1,
        "spread_end": 4,
        "spread_length": 3,
        "risk_unit": 35,
        "vol_tolerance": 1,
        "capital_distributions": [.33,.33,.33],
        "portfolio_cash": 100000,
        "portfolio_pct": 0.4
    },
    "CDBFP_1D": {
        "target_label": "one_min_vol",
        "spread_start": 1,
        "spread_end": 4,
        "spread_length": 3,
        "risk_unit": 35,
        "vol_tolerance": 1,
        "capital_distributions": [.33,.33,.33],
        "portfolio_cash": 100000,
        "portfolio_pct": 0.4
    },
    "CDGAINC_1D": {
        "target_label": "one_max_vol",
        "spread_start": 1,
        "spread_end": 4,
        "spread_length": 3,
        "risk_unit": 35,
        "vol_tolerance": 1,
        "capital_distributions": [.33,.33,.33],
        "portfolio_cash": 25000,
        "portfolio_pct": 0.4
    },
    "CDGAINP_1D": {
        "target_label": "one_min_vol",
        "spread_start": 1,
        "spread_end": 4,
        "spread_length": 3,
        "risk_unit": 35,
        "vol_tolerance": 1,
        "capital_distributions": [.33,.33,.33],
        "portfolio_cash": 25000,
        "portfolio_pct": 0.4

    },
    "CDLOSEC_1D": {
        "target_label": "one_max_vol",
        "spread_start": 1,
        "spread_end": 4,
        "spread_length": 3,
        "risk_unit": 35,
        "vol_tolerance": 1,
        "capital_distributions": [.33,.33,.33],
        "portfolio_cash": 25000,
        "portfolio_pct": 0.4
    },
    "CDLOSEP_1D": {
        "target_label": "one_min_vol",
        "spread_start": 1,
        "spread_end": 4,
        "spread_length": 3,
        "risk_unit": 35,
        "vol_tolerance": 1,
        "capital_distributions": [.33,.33,.33],
        "portfolio_cash": 25000,
        "portfolio_pct": 0.4

    },
}


