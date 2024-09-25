{% snapshot sf_account_snapshots %}

{{
    config(
      unique_key='Id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True
    )
}}


{# 引数を元に、参照するテーブルを決定 #}
{% set default_source_dataset, default_source_table = 'salesforce', 'Account' %}
--ref: {{ source(default_source_dataset, default_source_table) }}
{% set source_dataset = var('source_dataset', default_source_dataset) %}
{% set source_table = var('source_table', default_source_table) %}
{% set snapshot_dataset, snapshot_table = model.schema, model.name %}

{{ query_with_type_conversion(source_dataset,source_table,snapshot_dataset,snapshot_table) }}
{% endsnapshot %}