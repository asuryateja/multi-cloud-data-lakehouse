{{/*
  fct_events.sql
  Gold-layer fact table: business-aggregated daily event metrics.
  Joins events with customer dimension and transaction data across clouds.
  Materialized as a partitioned, clustered BigQuery table.

  Config override for BigQuery-specific optimizations.
*/}}

{{ config(
    materialized  = 'incremental',
    unique_key    = ['event_date', 'user_id', 'event_type'],
    partition_by  = {
        'field'       : 'event_date',
        'data_type'   : 'date',
        'granularity' : 'day',
    },
    cluster_by    = ['event_type', 'country_code', 'device_type'],
    incremental_strategy = 'merge',
    on_schema_change     = 'sync_all_columns',
    labels = {
        'layer'      : 'gold',
        'domain'     : 'clickstream',
        'pii'        : 'none',
    }
) }}

with

events as (
    select * from {{ ref('stg_events') }}
    {% if is_incremental() %}
        where event_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

-- Join events with customer attributes
events_enriched as (
    select
        e.event_id,
        e.event_date,
        e.event_timestamp,
        e.user_id,
        e.session_id,
        e.event_type,
        e.source_system,
        e.country_code,
        e.device_type,
        e.page_domain,
        e.page_url,
        e.referrer,

        -- Customer dimension attributes
        c.customer_segment,
        c.acquisition_channel,
        c.customer_tenure_days,
        c.is_premium_subscriber,

        -- Session-level aggregates (window functions)
        count(*) over (
            partition by e.session_id
        )                                           as session_event_count,

        row_number() over (
            partition by e.session_id
            order by e.event_timestamp
        )                                           as session_event_sequence,

        lag(e.event_type) over (
            partition by e.session_id
            order by e.event_timestamp
        )                                           as prev_event_type,

        timestamp_diff(
            e.event_timestamp,
            lag(e.event_timestamp) over (
                partition by e.session_id
                order by e.event_timestamp
            ),
            second
        )                                           as seconds_since_prev_event,

        e._ingestion_timestamp,
        e.dbt_updated_at

    from events e
    left join customers c using (user_id)
),

-- Attach same-day transaction flag
with_transactions as (
    select
        ev.*,
        coalesce(t.has_transaction, false)          as has_same_day_transaction,
        t.transaction_amount_usd
    from events_enriched ev
    left join (
        select
            customer_id                             as user_id,
            transaction_date,
            true                                    as has_transaction,
            sum(amount_usd)                         as transaction_amount_usd
        from transactions
        group by customer_id, transaction_date
    ) t on ev.user_id = t.user_id and ev.event_date = t.transaction_date
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['event_id']) }}    as event_key,
        event_id,
        session_id,
        user_id,

        -- Dates
        event_date,
        event_timestamp,

        -- Dimensions
        event_type,
        source_system,
        country_code,
        device_type,
        page_domain,

        -- Customer
        customer_segment,
        acquisition_channel,
        customer_tenure_days,
        is_premium_subscriber,

        -- Session behavior
        session_event_count,
        session_event_sequence,
        prev_event_type,
        seconds_since_prev_event,

        -- Conversion
        has_same_day_transaction,
        transaction_amount_usd,

        -- Metadata
        _ingestion_timestamp,
        dbt_updated_at

    from with_transactions
)

select * from final
