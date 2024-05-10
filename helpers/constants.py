PREFIXES = {
"GAIN": "invalerts-xgb-GAIN-classifier",
"GAIN_1D": "invalerts-xgb-GAIN-1d-classifier",
"GAINP": "invalerts-xgb-GAINP-classifier",
"GAINP_1D": "invalerts-xgb-GAINP-1d-classifier",
"LOSERS": "invalerts-xgb-LOSERS-classifier",
"LOSERS_1D": "invalerts-xgb-LOSERS-1d-classifier",
"LOSERSC": "invalerts-xgb-LOSERSC-classifier",
"LOSERSC_1D": "invalerts-xgb-LOSERSC-1d-classifier",
"MA": "invalerts-xgb-MA-classifier",
"MA_1D": "invalerts-xgb-MA-1d-classifier",
"MAP": "invalerts-xgb-MAP-classifier",
"MAP_1D": "invalerts-xgb-MAP-1d-classifier",
"CDBFC": "invalerts-xgb-CDBFC-classifier",
"CDBFC_1D": "invalerts-xgb-CDBFC-1d-classifier", 
"CDBFP": "invalerts-xgb-CDBFP-classifier",
"CDBFP_1D": "invalerts-xgb-CDBFP-1d-classifier",
# "VDIFFC": "invalerts-vdiffc-classifier",
# "VDIFFC_1D": "invalerts-vdiffc1d-classifier",
# "VDIFFP": "invalerts-vdiffp-classifier",
# "VDIFFP_1D": "invalerts-vdiffp1d-classifier",
# "IDXC": "invalerts-idxc-classifier",
# "IDXC_1D": "invalerts-idxc1d-classifier",
# "IDXP": "invalerts-idxp-classifier",
# "IDXP_1D": "invalerts-idxp1d-classifier",
}

TRADING_STRATEGIES = ["IDXC","IDXP","IDXC_1D","IDXP_1D","MA","MAP","MA_1D","MAP_1D","GAIN_1D","GAINP_1D","GAIN","GAINP","LOSERS","LOSERS_1D",
                      "LOSERSC","LOSERSC_1D","CDBFC","CDBFP","CDBFC_1D","CDBFP_1D","VDIFFC","VDIFFC_1D","VDIFFP","VDIFFP_1D"]

ACTIVE_STRATEGIES = [
    "CDBFC_1D","CDBFP_1D"
    ]

CALL_STRATEGIES = ["IDXC","IDXC_1D","MA","MA_1D","GAIN_1D","GAIN","LOSERSC","LOSERSC_1D","VDIFFC","VDIFFC_1D","CDBFC","CDBFC_1D"]
PUT_STRATEGIES = ["IDXP","IDXP_1D","MAP","MAP_1D","GAINP_1D","GAINP","LOSERS","LOSERS_1D","VDIFFP","VDIFFP_1D","CDBFP","CDBFP_1D"]
ONED_STRATEGIES = ["IDXC_1D","IDXP_1D","MA_1D","MAP_1D","GAIN_1D","GAINP_1D","LOSERS_1D","LOSERSC_1D","VDIFFC_1D","VDIFFP_1D","CDBFC_1D","CDBFP_1D"]
THREED_STRATEGIES = ["GAIN","GAINP","LOSERS","LOSERSC","CDBFC","CDBFP"]
CDVOL_STRATEGIES = ["CDBFC","CDBFP","CDBFC_1D","CDBFP_1D"]
TREND_STRATEGIES = ["GAIN_1D","GAINP_1D","GAIN","GAINP","LOSERS","LOSERS_1D","LOSERSC","LOSERSC_1D","MA","MA_1D","MAP","MAP_1D"]


ENDPOINT_NAMES = {
"GAIN": "invalerts-xgb-GAIN-classifier",
"GAIN_1D": "invalerts-xgb-GAIN-1d-classifier",
"GAINP": "invalerts-xgb-GAINP-classifier",
"GAINP_1D": "invalerts-xgb-GAINP-1d-classifier",
"LOSERS": "invalerts-xgb-LOSERS-classifier",
"LOSERS_1D": "invalerts-xgb-LOSERS-1d-classifier",
"LOSERSC": "invalerts-xgb-LOSERSC-classifier",
"LOSERSC_1D": "invalerts-xgb-LOSERSC-1d-classifier",
"MA": "invalerts-xgb-MA-classifier",
"MA_1D": "invalerts-xgb-MA-1d-classifier",
"MAP": "invalerts-xgb-MAP-classifier",
"MAP_1D": "invalerts-xgb-MAP-1d-classifier",
"CDBFC": "invalerts-xgb-CDBFC-classifier",
"CDBFC_1D": "invalerts-xgb-CDBFC-1d-classifier", 
"CDBFP": "invalerts-xgb-CDBFP-classifier",
"CDBFP_1D": "invalerts-xgb-CDBFP-1d-classifier",
# "VDIFFC": "invalerts-vdiffc-classifier",
# "VDIFFC_1D": "invalerts-vdiffc1d-classifier",
# "VDIFFP": "invalerts-vdiffp-classifier",
# "VDIFFP_1D": "invalerts-vdiffp1d-classifier",
# "IDXC": "invalerts-idxc-classifier",
# "IDXC_1D": "invalerts-idxc1d-classifier",
# "IDXP": "invalerts-idxp-classifier",
# "IDXP_1D": "invalerts-idxp1d-classifier",
}

ALGORITHM_CONFIG = {
    "CDBFC": {
        "target_label": "three_max_vol",
        "target_value": 1.731,
        "spread_start": 0,
        "spread_end": 2,
        "spread_length": 2,
        "risk_unit": 0.055,
        "vol_tolerance": 0.4,
    },
    "CDBFP": {
        "target_label": "three_min_vol",
        "target_value": -1.546,
        "spread_start": 0,
        "spread_end": 2,
        "spread_length": 2,
        "risk_unit": 0.055,
        "vol_tolerance": 0.4,
    },
    "CDBFC_1D": {
        "target_label": "one_max_vol",
        "target_value": 1.096,
        "spread_start": 1,
        "spread_end": 4,
        "spread_length": 3,
        "risk_unit": 0.055,
        "vol_tolerance": 0.4,
    },
    "CDBFP_1D": {
        "target_label": "one_min_vol",
        "target_value": -1.007,
        "spread_start": 1,
        "spread_end": 4,
        "spread_length": 3,
        "risk_unit": 0.055,
        "vol_tolerance": 0.4,
    },
}


