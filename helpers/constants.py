PREFIXES = {
"GAIN": "invalerts-gain-classifier",
"GAIN_1D": "invalerts-gain1d-classifier",
"GAINP": "invalerts-gainp-classifier",
"GAINP_1D": "invalerts-gainp1d-classifier",
"LOSERS": "invalerts-losers-classifier",
"LOSERS_1D": "invalerts-losers1d-classifier",
"LOSERSC": "invalerts-losersc-classifier",
"LOSERSC_1D": "invalerts-losersc1d-classifier",
"MA": "invalerts-ma-classifier",
"MA_1D": "invalerts-ma1d-classifier",
"MAP": "invalerts-map-classifier",
"MAP_1D": "invalerts-map1d-classifier",
"VDIFFC": "invalerts-vdiffc-classifier",
"VDIFFC_1D": "invalerts-vdiffc1d-classifier",
"VDIFFP": "invalerts-vdiffp-classifier",
"VDIFFP_1D": "invalerts-vdiffp1d-classifier",
"IDXC": "invalerts-idxc-classifier",
"IDXC_1D": "invalerts-idxc1d-classifier",
"IDXP": "invalerts-idxp-classifier",
"IDXP_1D": "invalerts-idxp1d-classifier",
}

TRADING_STRATEGIES = ["IDXC","IDXP","IDXC_1D","IDXP_1D","MA","MAP","MA_1D","MAP_1D","GAIN_1D","GAINP_1D","GAIN","GAINP","LOSERS","LOSERS_1D",
                      "LOSERSC","LOSERSC_1D","VDIFFC","VDIFFC_1D","VDIFFP_1D","VDIFFP"]

ACTIVE_STRATEGIES = ["GAIN_1D","GAINP_1D","GAIN","GAINP","LOSERS","LOSERS_1D","LOSERSC","LOSERSC_1D","MA","MA_1D","MAP","MAP_1D"]

CALL_STRATEGIES = ["IDXC","IDXC_1D","MA","MA_1D","GAIN_1D","GAIN","LOSERSC","LOSERSC_1D","VDIFFC","VDIFFC_1D"]

PUT_STRATEGIES = ["IDXP","IDXP_1D","MAP","MAP_1D","GAINP_1D","GAINP","LOSERS","LOSERS_1D","VDIFFP","VDIFFP_1D"]

ONED_STRATEGIES = ["IDXC_1D","IDXP_1D","MA_1D","MAP_1D","GAIN_1D","GAINP_1D","LOSERS_1D","LOSERSC_1D","VDIFFC_1D","VDIFFP_1D"]
THREED_STRATEGIES = ["IDXC","IDXP","MA","MAP","GAIN","GAINP","LOSERS","LOSERSC","VDIFFC","VDIFFP"]


ENDPOINT_NAMES = {
"GAIN": "invalerts-gain-classifier",
"GAIN_1D": "invalerts-gain1d-classifier",
"GAINP": "invalerts-gainp-classifier",
"GAINP_1D": "invalerts-gainp1d-classifier",
"LOSERS": "invalerts-losers-classifier",
"LOSERS_1D": "invalerts-losers1d-classifier",
"LOSERSC": "invalerts-losersc-classifier",
"LOSERSC_1D": "invalerts-losersc1d-classifier",
"MA": "invalerts-ma-classifier",
"MA_1D": "invalerts-ma1d-classifier",
"MAP": "invalerts-map-classifier",
"MAP_1D": "invalerts-map1d-classifier",
"VDIFFC": "invalerts-vdiffc-classifier",
"VDIFFC_1D": "invalerts-vdiffc1d-classifier",
"VDIFFP": "invalerts-vdiffp-classifier",
"VDIFFP_1D": "invalerts-vdiffp1d-classifier",
"IDXC": "invalerts-idxc-classifier",
"IDXC_1D": "invalerts-idxc1d-classifier",
"IDXP": "invalerts-idxp-classifier",
"IDXP_1D": "invalerts-idxp1d-classifier",
}

ALGORITHM_CONFIG = {
    "GAIN_1D": {
        "target_label": "one_max",
        "target_value": .0175,
    },
    "GAIN": {
        "target_label": "three_max",
        "target_value": .0275,
    },
    "GAINP_1D": {
        "target_label": "one_min",
        "target_value": -.018,
    },
    "GAINP": {
        "target_label": "three_min",
        "target_value": -.028,
    },
     "LOSERSC_1D": {
        "target_label": "one_max",
        "target_value": .016,
    },
    "LOSERSC": {
        "target_label": "three_max",
        "target_value": .025,
    },
    "LOSERS_1D": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "LOSERS": {
        "target_label": "three_min",
        "target_value": -.02,
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
        "target_value": .013,
    },
    "MA": {
        "target_label": "three_max",
        "target_value": .028,
    },
    "MAP_1D": {
        "target_label": "one_min",
        "target_value": -.013,
    },
    "MAP": {
        "target_label": "three_min",
        "target_value": -.028,
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
