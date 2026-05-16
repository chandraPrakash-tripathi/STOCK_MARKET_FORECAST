-- models/staging/stg_news_articles.sql
{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}


with source as (
    select * from {{ source('raw', 'news_articles') }}
),

cleaned as (
    select
        id                                           as article_id,
        source                                       as publisher,
        lower(trim(title))                           as title,
        lower(trim(coalesce(description, '')))       as description,
        url,
        published_at                                 as published_at,
        -- combine title + description as the sentiment input text
        trim(lower(title) || ' ' || lower(coalesce(description, '')))
                                                     as sentiment_text,
        -- date only for joining to price data
        published_at::date                           as article_date,
        ingested_at

    from source
    where
        title is not null
        and published_at is not null
)

select * from cleaned