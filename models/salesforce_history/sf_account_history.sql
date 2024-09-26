{# 
手軽に試すなら、materialized=table か view が良いでしょう
{{ config(
    materialized='view',
) }}
 #}
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

{# start_dateの指定日以降の履歴テーブルを作成します。※ 省略可能 #}
{{ transform_snapshot_to_timeseries(ref('sf_account_snapshots'), start_date='2024-01-01')}}
