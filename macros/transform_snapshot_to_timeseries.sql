{% macro transform_snapshot_to_timeseries(table_ref, start_date, primary_key='Id') -%}
with source as (
    select * from {{ table_ref }}
), date_spine as (
  select date
  from unnest(generate_date_array(
    (select min(date(dbt_valid_from, 'Asia/Tokyo')) from source),
    (select max(coalesce(date(dbt_valid_to, 'Asia/Tokyo'), current_date('Asia/Tokyo'))) from source),
    interval 1 day
  )) as date
), hist as (
  select
      *
  from
      source
  qualify
    row_number() over (
      partition by {{ primary_key }}, date(dbt_valid_from, 'Asia/Tokyo')
      order by dbt_valid_from desc
    ) = 1
)

select 
    date_spine.date as dim_date_jst,
    hist.*
from
    date_spine
    join hist
        on date_spine.date >= date(hist.dbt_valid_from, 'Asia/Tokyo')
        and (date_spine.date < date(hist.dbt_valid_to, 'Asia/Tokyo') or hist.dbt_valid_to is null)
where
    date_spine.date > '{{ start_date }}'
    and date_spine.date < current_date('Asia/Tokyo')
{%- if is_incremental() %}
    and date_spine.date >= date_sub(current_date('Asia/Tokyo'), interval 7 day)
{% endif %}
{%- endmacro %}