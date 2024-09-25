{{ config(
    tags=["sfdc-daily"],
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "dim_date_jst",
      "data_type": "date",
      "granularity": "day"
    },
) }}

{{ transform_snapshot_to_timeseries(ref('sf_account_snapshots'), '2022-12-06')}}
