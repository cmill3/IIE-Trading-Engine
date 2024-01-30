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
                      "LOSERSC","LOSERSC_1D","VDIFFC","VDIFFC_1D","VDIFFP_1D","VDIFFP"]

ACTIVE_STRATEGIES = ["GAIN_1D","GAINP_1D","GAIN","GAINP","LOSERS","LOSERS_1D","LOSERSC","LOSERSC_1D","MA","MA_1D","MAP","MAP_1D"]

CALL_STRATEGIES = ["IDXC","IDXC_1D","MA","MA_1D","GAIN_1D","GAIN","LOSERSC","LOSERSC_1D","VDIFFC","VDIFFC_1D"]
PUT_STRATEGIES = ["IDXP","IDXP_1D","MAP","MAP_1D","GAINP_1D","GAINP","LOSERS","LOSERS_1D","VDIFFP","VDIFFP_1D"]
ONED_STRATEGIES = ["IDXC_1D","IDXP_1D","MA_1D","MAP_1D","GAIN_1D","GAINP_1D","LOSERS_1D","LOSERSC_1D","VDIFFC_1D","VDIFFP_1D"]
THREED_STRATEGIES = ["IDXC","IDXP","MA","MAP","GAIN","GAINP","LOSERS","LOSERSC","VDIFFC","VDIFFP"]


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
        "target_value": .014,
    },
    "GAIN": {
        "target_label": "three_max",
        "target_value": .022,
    },
    "GAINP_1D": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "GAINP": {
        "target_label": "three_min",
        "target_value": -.02,
    },
     "LOSERSC_1D": {
        "target_label": "one_max",
        "target_value": .017,
    },
    "LOSERSC": {
        "target_label": "three_max",
        "target_value": .026,
    },
    "LOSERS_1D": {
        "target_label": "one_min",
        "target_value": -.015,
    },
    "LOSERS": {
        "target_label": "three_min",
        "target_value": -.023,
    },
    "vdiffC1d": {
        "target_label": "one_max",
        "target_value": .013,
    },
    "vdiffC": {
        "target_label": "three_max",
        "target_value": .028,
    },
    "vdiffP1d": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "vdiffP": {
        "target_label": "three_min",
        "target_value": -.028,
    },
     "MA_1D": {
        "target_label": "one_max",
        "target_value": .014,
    },
    "MA": {
        "target_label": "three_max",
        "target_value": .021,
    },
    "MAP_1D": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "MAP": {
        "target_label": "three_min",
        "target_value": -.021,
    },
    "idxC1d": {
        "target_label": "one_max",
        "target_value": .013,
    },
    "idxC": {
        "target_label": "three_max",
        "target_value": .028,
    },
    "idxP1d": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "idxP": {
        "target_label": "three_min",
        "target_value": -.028,
    },
}
