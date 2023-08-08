{{ config(materialized='table') }}

with 
    q_page_title as (
        select key, value__string_value, _dlt_parent_id 
            from bigquery_data.events__event_params
            where key=='page_title'
    ),
    q_page_view_event as (
        select cast(concat(substring(event_date,1,4),'-',substring(event_date,5,2),'-',substring(event_date,7,2)) as date) as date, event_name, _dlt_id
            from bigquery_data.events
            where event_name=='page_view'
    )

select *
    from q_page_view_event
        left join q_page_title
            on q_page_view_event._dlt_id = q_page_title._dlt_parent_id