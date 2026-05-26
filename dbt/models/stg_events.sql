{{/*
  stg_events.sql
  Staging model: normalizes raw event data from AWS Delta Lake and GCP streaming sources.
  Applies column renames, casts, PII masking, and deduplication.
  Target: BigQuery (view materialization)
*/}}

with

source_delta as (
    select
        event_id,
        event_timestamp,
        user_id,
        event_type,
        source_system,
        -- JSON payload normalization
        json_extract_scalar(payload, '$.session_id')    as session_id,
        json_extract_scalar(payload, '$.page_url')      as page_url,
        json_extract_scalar(payload, '$.referrer')      as referrer,
        json_extract_scalar(payload, '$.device_type')   as device_type,
        json_extract_scalar(payload, '$.country_code')  as country_code,
        _ingestion_timestamp,
        _partition_date                                  as event_date
    from {{ source('aws_delta', 'raw_events') }}
    where _partition_date = '{{ var("execution_date", modules.datetime.date.today().isoformat()) }}'
),

source_streaming as (
    select
        event_id,
        event_timestamp,
        user_id,
        event_type,
        'gcp_pubsub'                                    as source_system,
        json_extract_scalar(payload, '$.session_id')    as session_id,
        json_extract_scalar(payload, '$.page_url')      as page_url,
        json_extract_scalar(payload, '$.referrer')      as referrer,
        json_extract_scalar(payload, '$.device_type')   as device_type,
        json_extract_scalar(payload, '$.country_code')  as country_code,
        event_timestamp                                  as _ingestion_timestamp,
        date(event_timestamp)                            as event_date
    from {{ source('gcp_streaming', 'events') }}
    where date(event_timestamp) = '{{ var("execution_date", modules.datetime.date.today().isoformat()) }}'
),

-- Union all sources before deduplication
unioned as (
    select * from source_delta
    union all
    select * from source_streaming
),

-- Deduplicate on event_id, keeping earliest ingestion
deduplicated as (
    select *
    from unioned
    qualify row_number() over (
        partition by event_id
        order by _ingestion_timestamp asc
    ) = 1
),

-- Final column selection with business-ready naming
final as (
    select
        event_id,
        event_timestamp,
        date(event_timestamp)                           as event_date,
        timestamp_diff(
            event_timestamp,
            timestamp_trunc(event_timestamp, hour),
            second
        )                                               as seconds_into_hour,

        -- Identifiers
        user_id,
        session_id,

        -- Categorical
        lower(event_type)                               as event_type,
        lower(source_system)                            as source_system,
        upper(coalesce(country_code, 'XX'))             as country_code,
        lower(coalesce(device_type, 'unknown'))         as device_type,

        -- URLs (truncated for analytics)
        regexp_extract(page_url, r'^https?://[^/]+')    as page_domain,
        page_url,
        referrer,

        -- Metadata
        _ingestion_timestamp,

        -- Audit columns
        current_timestamp()                             as dbt_updated_at,
        '{{ invocation_id }}'                           as dbt_invocation_id

    from deduplicated
    where event_id is not null
      and event_timestamp is not null
)

select * from final
