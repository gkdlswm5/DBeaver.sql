--af app opened 13868, first_far 4872, registration 15001, pageview_to_extension 15002, af_complete_registration 4871, first_tar 13870
set timezone= 'America/New_York'

--linear
with linear as (
    select
        'linear' as type,
        slm.broadcast_week,
        slm.airdate,
        slm.slug,
        slm.creative,
        slm.net,
        slm.daypart,
        sum(case when c.primary_conversion_metric_id=slm.metric_id then slm.spend end) as spend,
        --af app opened 13868
        sum(case when cm.id=13868 then slm.lift end) as app_opened,
        0 as uf_lift_1dvt,
        0 as uf_lift_7dvt,
        0 as uf_lift_28dvt,
        --first_far 4872
        sum(case when cm.id=4872 then slm.lift end) as first_far,
        0 as df1_lift_1dvt,
        0 as df1_lift_7dvt,
        0 as df1_lift_28dvt,
        --registration 15001
        sum(case when cm.id=15001 then slm.lift end) as registration,
        0 as df2_lift_1dvt,
        0 as df2_lift_7dvt,
        0 as df2_lift_28dvt,
        --pageview_to_extension 15002
        sum(case when cm.id=15002 then slm.lift end) as pageview_to_extension,
        0 as df3_lift_1dvt,
        0 as df3_lift_7dvt,
        0 as df3_lift_28dvt,
        --af_complete_registration 4871
        sum(case when cm.id=4871 then slm.lift end) as af_complete_registration,
        0 as df4_lift_1dvt,
        0 as df4_lift_7dvt,
        0 as df4_lift_28dvt,
        --first_tar 13870
        sum(case when cm.id=13870 then slm.lift end) as first_tar,
        0 as df5_lift_1dvt,
        0 as df5_lift_7dvt,
        0 as df5_lift_28dvt,
        sum(case when c.primary_conversion_metric_id=slm.metric_id then coalesce(s.impressions,s.impressions_predicted) end) as impressions
    from core.spot_level_metrics slm
    left join philo.spots as s on slm.spot_id = s.id
    left join philo.conversion_metrics cm on slm.metric_id=cm.id
    left join philo.companies c on slm.company_id = c.id
    --change based on client
    where slm.slug='ibotta' and slm.broadcast_week>='2021-01-01' and slm.broadcast_week<= '2021-01-02'
    group by 1,2,3,4,5,6,7),

--streaming
streaming as (
    select 'streaming' as type,
           date_trunc('week', csr.viewed_day)::date as broadcast_week,
           date_trunc('day', csr.viewed_day)::date as airdate,
           csr.company_slug as slug,
           csr.creative_name as creative,
           csr.publisher_friendly_name as network,
           'none' as daypart,
           sum(case when c.primary_conversion_metric_id=csr.metric_id and csr.attribution_method='incremental' then csr.spend end) as spend,
           --af_app_opened 13868
           sum(case when cm.id=13868 and csr.attribution_method='incremental' then csr.total_lift end) as app_opened_inc,
           sum(case when cm.id=13868 and csr.attribution_method='view_through_day' then csr.total_lift end) as app_opened_1dvt,
           sum(case when cm.id=13868 and csr.attribution_method='view_through_week' then csr.total_lift end) as app_opened_7dvt,
           sum(case when cm.id=13868 and csr.attribution_method='view_through_month' then csr.total_lift end) as app_opened_28dvt,
           --first_far 4872
           sum(case when cm.id=4872 and csr.attribution_method='incremental' then csr.total_lift end) as first_far_inc,
           sum(case when cm.id=4872 and csr.attribution_method='view_through_day' then csr.total_lift end) as first_far_1dvt,
           sum(case when cm.id=4872 and csr.attribution_method='view_through_week' then csr.total_lift end) as first_far_7dvt,
           sum(case when cm.id=4872 and csr.attribution_method='view_through_month' then csr.total_lift end) as first_far_28dvt,
           --registration 15001
           sum(case when cm.id=15001 and csr.attribution_method='incremental' then csr.total_lift end) as registration_inc,
           sum(case when cm.id=15001 and csr.attribution_method='view_through_day' then csr.total_lift end) as registration_1dvt,
           sum(case when cm.id=15001 and csr.attribution_method='view_through_week' then csr.total_lift end) as registration_7dvt,
           sum(case when cm.id=15001 and csr.attribution_method='view_through_month' then csr.total_lift end) as registration_28dvt,
           --pageview_to_extension 15002
           sum(case when cm.id=15002 and csr.attribution_method='incremental'then csr.total_lift end) as pageview_to_extension_inc,
           sum(case when cm.id=15002 and csr.attribution_method='view_through_day'then csr.total_lift end) as pageview_to_extension_1dvt,
           sum(case when cm.id=15002 and csr.attribution_method='view_through_week'then csr.total_lift end) as pageview_to_extension_7dvt,
           sum(case when cm.id=15002 and csr.attribution_method='view_through_month'then csr.total_lift end) as pageview_to_extension_28dvt,
           --af_complete_registration 4871
           sum(case when cm.id=4871 and csr.attribution_method='incremental' then csr.total_lift end) as af_complete_registration_inc,
           sum(case when cm.id=4871 and csr.attribution_method='view_through_day' then csr.total_lift end) as af_complete_registration_1dvt,
           sum(case when cm.id=4871 and csr.attribution_method='view_through_week' then csr.total_lift end) as af_complete_registration_7dvt,
           sum(case when cm.id=4871 and csr.attribution_method='view_through_month' then csr.total_lift end) as af_complete_registration_28dvt,
           --first_tar 13870
           sum(case when cm.id=13870 and csr.attribution_method='incremental' then csr.total_lift end) as first_tar_inc,
           sum(case when cm.id=13870 and csr.attribution_method='view_through_day' then csr.total_lift end) as first_tar_1dvt,
           sum(case when cm.id=13870 and csr.attribution_method='view_through_week' then csr.total_lift end) as first_tar_7dvt,
           sum(case when cm.id=13870 and csr.attribution_method='view_through_month' then csr.total_lift end) as first_tar_28dvt,
           sum(case when csr.metric_id = c.primary_conversion_metric_id and csr.attribution_method = 'incremental' then csr.impression_counts end) as impressions
    from core.core_streaming_report csr
    left join philo.conversion_metrics cm on csr.metric_id=cm.id
    join philo.companies c on csr.company_id = c.id
    where csr.company_slug='ibotta' and csr.viewed_day>='2021-01-01' and csr.viewed_day<='2021-01-02'
    group by 1,2,3,4,5,6,7)
