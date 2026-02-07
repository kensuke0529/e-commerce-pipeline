{% snapshot order_snapshot %}

{{
    config(
      target_schema='snapshots',
    )
}}

select * from {{ ref('fct_orders') }}

{% endsnapshot %}
