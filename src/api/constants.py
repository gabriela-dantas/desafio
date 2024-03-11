BPM_OPT_IN_ID = 318196139

type_etl = {
    "POST /pricingQuery": "from_api_to_model_itau_md_cota",
    "POST /quotas": "from_api_to_model_itau_md_cota",
    "POST /object/create": "porto_customer_ingestion",
    "POST /contract/update-contract": "porto_quota_reactivation",
}
