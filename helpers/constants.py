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
    # "GAIN_1D","GAINP_1D","GAIN","GAINP","LOSERS","LOSERS_1D","LOSERSC","LOSERSC_1D",
    "CDBFC","CDBFP","CDBFC_1D","CDBFP_1D"
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
    "GAIN_1D": {
        "target_label": "one_max",
        "target_value": .016,
    },
    "GAIN": {
        "target_label": "three_max",
        "target_value": .026,
    },
    "GAINP_1D": {
        "target_label": "one_min",
        "target_value": -.017,
    },
    "GAINP": {
        "target_label": "three_min",
        "target_value": -.026,
    },
     "LOSERSC_1D": {
        "target_label": "one_max",
        "target_value": .02,
    },
    "LOSERSC": {
        "target_label": "three_max",
        "target_value": .032,
    },
    "LOSERS_1D": {
        "target_label": "one_min",
        "target_value": -.02,
    },
    "LOSERS": {
        "target_label": "three_min",
        "target_value": -.032,
    },
     "MA_1D": {
        "target_label": "one_max",
        "target_value": .017,
    },
    "MA": {
        "target_label": "three_max",
        "target_value": .026,
    },
    "MAP_1D": {
        "target_label": "one_min",
        "target_value": -.017,
    },
    "MAP": {
        "target_label": "three_min",
        "target_value": -.026,
    },
    "CDBFC": {
        "target_label": "three_max_vol",
        "target_value": 1.731,
    },
    "CDBFP": {
        "target_label": "three_min_vol",
        "target_value": -1.554,
        
    },
    "CDBFC_1D": {
        "target_label": "one_max_vol",
        "target_value": 1.098,
    },
    "CDBFP_1D": {
        "target_label": "one_min_vol",
        "target_value": -1.012,
    },
}
