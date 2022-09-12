--add ID in case when
with linear as (
    select
        'linear' as type,
        slm.spot_id as sid,
        slm.broadcast_week,
        slm.airdate,
        slm.slug,
        slm.creative,
        slm.net,
        slm.daypart,
        sum(case when slm.metric = 'uvs' then slm.spend end) as spend,
        sum(case when slm.metric = 'uvs' then coalesce(s.impressions, s.impressions_predicted) end) as impressions,
        sum(case when slm.metric in ('installs') then slm.lift end) as install_lift_inc,
        0 as install_lift_1dvt ,
        0 as install_lift_7dvt ,
        0 as install_lift_28dvt,
        sum(case when slm.metric in ('uvs') then slm.lift end) as uv_lift_inc,
        0 as uv_lift_1dvt ,
        0 as uv_lift_7dvt ,
        0 as uv_lift_28dvt,
        sum(case when slm.metric in ('first_far') then slm.lift end) as df_metric_1_conversions_inc, --change by client
        0 as df_metric_2_conversions_1dvt ,
        0 as df_metric_2_conversions_7dvt ,
        0 as df_metric_2_conversions_28dvt,
        sum(case when slm.metric in ('ibotta_pageview_to_Registration') then slm.lift end) as df_metric_2_conversions_inc, --change by client
        0 as df_metric_2_conversions_1dvt ,
        0 as df_metric_2_conversions_7dvt ,
        0 as df_metric_2_conversions_28dvt,
        sum(case when slm.metric in ( 'ibotta_pageview_to_extension_installed' ) then slm.lift end) as df_metric_3_conversions_inc, --(changes by client)
        0 as df_metric_3_conversions_1dvt ,
        0 as df_metric_3_conversions_7dvt ,
        0 as df_metric_3_conversions_28dvt,
        sum(case when slm.metric in ( 'af_complete_registration' ) then slm.lift end) as df_metric_4_conversions_inc, --(changes by client)
        0 as df_metric_4_conversions_1dvt ,
        0 as df_metric_4_conversions_7dvt ,
        0 as df_metric_4_conversions_28dvt,
        sum(case when slm.metric in ( 'first_tar' ) then slm.lift end) as df_metric_5_conversions_inc, --(changes by client)
        0 as df_metric_5_conversions_1dvt,
        0 as df_metric_5_conversions_7dvt,
        0 as df_metric_5_conversions_28dvt
from core.spot_level_metrics slm
left join philo.spots s on slm.spot_id = s.id
where csr.company_slug = 'ibotta'
group by 1,2,3,4,5,6,7,8),

--first far (ALL) id = 4872
--
ses as (
    select
        'streaming' as type,
        csr.campaign_id,
        date_trunc('week', csr.viewed_day)::date as broadcast_week,
        date_trunc('day', csr.viewed_day)::date as airdate,
        --delete
        csr.company_id,
        csr.company_slug as slug,
        csr.creative_name as creative,
        csr.publisher_friendly_name as network,
        'none' as daypart,
        sum(case when c.primary_conversion_metric_id=csr.metric_id and csr.attribution_method='incremental' then csr.spend end) as spend,

    from core.core_streaming_report csr
    left join philo.conversion_metrics cm on csr.metric_id = cm.id
    join philo.companies c on csr.company_id = c.id
    where csr.company_slug = 'ibotta'
)
