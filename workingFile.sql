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
group by 1,2,3,4,5,6,7,8),
