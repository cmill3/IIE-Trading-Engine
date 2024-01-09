PREFIXES = {
    "GAIN": "invalerts-xgb-bfc-classifier",
    "GAIN_1D": "invalerts-xgb-bfp-classifier",
    "GAINP":  "invalerts-xgb-bfc-1d-classifier",
    "GAINP_1D": "invalerts-xgb-bfp-1d-classifier",
    "LOSERS": "invalerts-xgb-bfc-classifier",
    "LOSERS_1D": "invalerts-xgb-bfp-classifier",
    "LOSERSC":  "invalerts-xgb-bfc-1d-classifier",
    "LOSERSC_1D": "invalerts-xgb-bfp-1d-classifier",
    "MA": "invalerts-xgb-bfc-classifier",
    "MA_1D": "invalerts-xgb-bfp-classifier",
    "MAP":  "invalerts-xgb-bfc-1d-classifier",
    "MAP_1D": "invalerts-xgb-bfp-1d-classifier",
    "VDIFFP": "invalerts-xgb-bfc-classifier",
    "VDIFFP_1D": "invalerts-xgb-bfp-classifier",
    "VDIFFC":  "invalerts-xgb-bfc-1d-classifier",
    "VDIFFC_1D": "invalerts-xgb-bfp-1d-classifier",
    "IDXC": "invalerts-xgb-indexc-classifier",
    "IDXP": "invalerts-xgb-indexp-classifier",
    "IDXC_1D":  "invalerts-xgb-indexc-1d-classifier",
    "IDXP_1D": "invalerts-xgb-indexp-1d-classifier",
}

TRADING_STRATEGIES = ["IDXC","IDXP","IDXC_1D","IDXP_1D","MA","MAP","MA_1D","MAP_1D","GAIN_1D","GAINP_1D","GAIN","GAINP","LOSERS","LOSERS_1D",
                      "LOSERSC","LOSERSC_1D","VDIFFC","VDIFFC_1D","VDIFFP_1D","VDIFFP"]

ACTIVE_STRATEGIES = ["GAIN_1D","GAINP_1D","GAIN","GAINP","LOSERS","LOSERS_1D","LOSERSC","LOSERSC_1D"]

CALL_STRATEGIES = ["IDXC","IDXC_1D","MA","MA_1D","GAIN_1D","GAIN","LOSERSC","LOSERSC_1D","VDIFFC","VDIFFC_1D"]

PUT_STRATEGIES = ["IDXP","IDXP_1D","MAP","MAP_1D","GAINP_1D","GAINP","LOSERS","LOSERS_1D","VDIFFP","VDIFFP_1D"]


ALGORITHM_CONFIG = {
    "GAIN": "invalerts-xgb-bfc-classifier",
    "GAIN_1D": "invalerts-xgb-bfp-classifier",
    "GAINP":  "invalerts-xgb-bfc-1d-classifier",
    "GAINP_1D": "invalerts-xgb-bfp-1d-classifier",
    "LOSERS": "invalerts-xgb-bfc-classifier",
    "LOSERS_1D": "invalerts-xgb-bfp-classifier",
    "LOSERSC":  "invalerts-xgb-bfc-1d-classifier",
    "LOSERSC_1D": "invalerts-xgb-bfp-1d-classifier",
    "MA": "invalerts-xgb-bfc-classifier",
    "MA_1D": "invalerts-xgb-bfp-classifier",
    "MAP":  "invalerts-xgb-bfc-1d-classifier",
    "MAP_1D": "invalerts-xgb-bfp-1d-classifier",
    "VDIFFP": "invalerts-xgb-bfc-classifier",
    "VDIFFP_1D": "invalerts-xgb-bfp-classifier",
    "VDIFFC":  "invalerts-xgb-bfc-1d-classifier",
    "VDIFFC_1D": "invalerts-xgb-bfp-1d-classifier",
    "IDXC": "invalerts-xgb-indexc-classifier",
    "IDXP": "invalerts-xgb-indexp-classifier",
    "IDXC_1D":  "invalerts-xgb-indexc-1d-classifier",
    "IDXP_1D": "invalerts-xgb-indexp-1d-classifier",
}
