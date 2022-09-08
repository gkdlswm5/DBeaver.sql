with linear as (
select 'linear' as type,  spot_id as sid, broadcast_week, airdate, slug, creative, net, daypart,
sum(case when metric = 'uvs' then slm.spend end) as spend,
sum(case when metric = 'uvs' then coalesce(s.impressions, s.impressions_predicted) end) as impressions
– in JSON run a for loop to see if the client looks at uvs / installs or 1 of the following
, sum(case when metric in ( 'installs'  ) then slm.lift end) as install_lift_inc,
0 as install_lift_1dvt ,
0 as install_lift_7dvt ,
0 as install_lift_28dvt
, sum(case when metric in ( 'uvs'  ) then slm.lift end) as uv_lift_inc,
0 as uv_lift_1dvt ,
0 as uv_lift_7dvt ,
0 as uv_lift_28dvt
, sum(case when metric in ( 'first_far' ) then slm.lift end) as df_metric_1_conversions_inc, – (changes by client)
0 as df_metric_1_conversions_1dvt ,
0 as df_metric_1_conversions_7dvt ,
0 as df_metric_1_conversions_28dvt
, sum(case when metric in ( 'ibotta_pageview_to_Registration' ) then slm.lift end) as df_metric_2_conversions_inc, – (changes by client)
0 as df_metric_2_conversions_1dvt ,
0 as df_metric_2_conversions_7dvt ,
0 as df_metric_2_conversions_28dvt
, sum(case when metric in ( 'ibotta_pageview_to_extension_installed' ) then slm.lift end) as df_metric_3_conversions_inc, – (changes by client)
0 as df_metric_3_conversions_1dvt ,
0 as df_metric_3_conversions_7dvt ,
0 as df_metric_3_conversions_28dvt
, sum(case when metric in ( 'af_complete_registration' ) then slm.lift end) as df_metric_4_conversions_inc, – (changes by client)
0 as df_metric_4_conversions_1dvt ,
0 as df_metric_4_conversions_7dvt ,
0 as df_metric_4_conversions_28dvt
, sum(case when metric in ( 'first_tar' ) then slm.lift end) as df_metric_5_conversions_inc, – (changes by client)
0 as df_metric_5_conversions_1dvt ,
0 as df_metric_5_conversions_7dvt ,
0 as df_metric_5_conversions_28dvt
– UV SWITCH NOWh
from bi.spot_level_metrics slm
left join philo.spots s on slm.spot_id = S.id - The Shortest Link Shortener and Microsite builder 
where slug = 'ibotta'
group by 1,2,3,4,5,6,7,8
),
– NEW STREAMING ATTEMPT
ses as (
select 'streaming' as type , svivppf.campaign_id, date_trunc('week' , svivppf.view_date_et)::date as broadcast_week ,
svivppf.view_date_et, svivppf.client_id , client_slug as slug,  svivppf.creative_id, svivppf.publisher_id,'none' as daypart,
sum(case when sesspf.session_type = 'web' then svivppf.spend end) as spend,
sum(case when sesspf.session_type = 'web' then svivppf.impression_count end) as impressions
—
, sum(case when sesspf.session_type = 'app' then sesspf.incremental_session_lift end) as install_lift_inc, – (changes by client)
sum(case when sesspf.session_type = 'app' then sesspf.viewthrough_1d_session_lift end) as install_lift_1dvt, – (changes by client)
sum(case when sesspf.session_type = 'app' then sesspf.viewthrough_7d_session_lift end) as install_lift_7dvt, – (changes by client)
sum(case when sesspf.session_type = 'app' then sesspf.viewthrough_28d_session_lift end) as install_lift_28dvt – (changes by client)
, sum(case when sesspf.session_type = 'web' then sesspf.incremental_session_lift end) as uv_lift_inc, – (changes by client)
sum(case when sesspf.session_type = 'web' then sesspf.viewthrough_1d_session_lift end) as uv_lift_1dvt, – (changes by client)
sum(case when sesspf.session_type = 'web' then sesspf.viewthrough_7d_session_lift end) as uv_lift_7dvt, – (changes by client)
sum(case when sesspf.session_type = 'web' then sesspf.viewthrough_28d_session_lift end) as uv_lift_28dvt – (changes by client)
from bi.spend_vs_impression_vs_primary_performance_facts svivppf
left join bi.client c on svivppf.client_id = c.id
full outer join  bi.session_performance_facts sesspf
on svivppf.campaign_id = sesspf.campaign_id
and svivppf.creative_id = sesspf.creative_id
and svivppf.view_date_et = sesspf.view_date_et
and svivppf.publisher_id = sesspf.publisher_id
and svivppf.client_id = sesspf.client_id
where c.client_slug = 'ibotta'
group by 1,2,3,4,5,6,7,8),
sales as (
select campaign_id, creative_id, view_date_et, publisher_id, client_id,
– changes by metrics depending on the client sales type
sum(case when spf.sale_type = 'First Far (All)' and spf.session_type = 'app-action'  then incremental_sale_lift end) as df_metric_1_conversions_inc, – (changes by client)
sum(case when spf.sale_type = 'First Far (All)' and spf.session_type = 'app-action' then viewthrough_1d_sale_lift end) as df_metric_1_conversions_1dvt, – (changes by client)
sum(case when spf.sale_type = 'First Far (All)' and spf.session_type = 'app-action'  then viewthrough_7d_sale_lift end) as df_metric_1_conversions_7dvt, – (changes by client)
sum(case when spf.sale_type = 'First Far (All)' and spf.session_type = 'app-action' then viewthrough_28d_sale_lift end) as df_metric_1_conversions_28dvt, – (changes by client)
sum(case when spf.sale_type = 'Registration' and spf.session_type = 'web-action'  then incremental_sale_lift end) as df_metric_2_conversions_inc, – (changes by client)
sum(case when spf.sale_type = 'Registration' and spf.session_type = 'web-action' then viewthrough_1d_sale_lift end) as df_metric_2_conversions_1dvt, – (changes by client)
sum(case when spf.sale_type = 'Registration' and spf.session_type = 'web-action'  then viewthrough_7d_sale_lift end) as df_metric_2_conversions_7dvt, – (changes by client)
sum(case when spf.sale_type = 'Registration' and spf.session_type = 'web-action' then viewthrough_28d_sale_lift end) as df_metric_2_conversions_28dvt, – (changes by client)
sum(case when spf.sale_type = 'Extension Installed' and spf.session_type = 'web-action'  then incremental_sale_lift end) as df_metric_3_conversions_inc, – (changes by client)
sum(case when spf.sale_type = 'Extension Installed' and spf.session_type = 'web-action' then viewthrough_1d_sale_lift end) as df_metric_3_conversions_1dvt, – (changes by client)
sum(case when spf.sale_type = 'Extension Installed' and spf.session_type = 'web-action'  then viewthrough_7d_sale_lift end) as df_metric_3_conversions_7dvt, – (changes by client)
sum(case when spf.sale_type = 'Extension Installed' and spf.session_type = 'web-action' then viewthrough_28d_sale_lift end) as df_metric_3_conversions_28dvt, – (changes by client)
sum(case when spf.sale_type = 'af_complete_registration' and spf.session_type = 'app-action'  then incremental_sale_lift end) as df_metric_4_conversions_inc, – (changes by client)
sum(case when spf.sale_type = 'af_complete_registration' and spf.session_type = 'app-action' then viewthrough_1d_sale_lift end) as df_metric_4_conversions_1dvt, – (changes by client)
sum(case when spf.sale_type = 'af_complete_registration' and spf.session_type = 'app-action'  then viewthrough_7d_sale_lift end) as df_metric_4_conversions_7dvt, – (changes by client)
sum(case when spf.sale_type = 'af_complete_registration' and spf.session_type = 'app-action' then viewthrough_28d_sale_lift end) as df_metric_4_conversions_28dvt, – (changes by client)
sum(case when spf.sale_type = 'First Tar (All)' and spf.session_type = 'app-action'  then incremental_sale_lift end) as df_metric_5_conversions_inc, – (changes by client)
sum(case when spf.sale_type = 'First Tar (All)' and spf.session_type = 'app-action' then viewthrough_1d_sale_lift end) as df_metric_5_conversions_1dvt, – (changes by client)
sum(case when spf.sale_type = 'First Tar (All)' and spf.session_type = 'app-action'  then viewthrough_7d_sale_lift end) as df_metric_5_conversions_7dvt, – (changes by client)
sum(case when spf.sale_type = 'First Tar (All)' and spf.session_type = 'app-action' then viewthrough_28d_sale_lift end) as df_metric_5_conversions_28dvt
– (changes by client)
from bi.sale_performance_facts spf
left join bi.client c on spf.client_id = c.id
where client_slug = 'ibotta' – (changes by client)
group by 1,2,3,4,5
),
streaming as (
select ses.type, ses.campaign_id, ses.broadcast_week, ses.view_date_et, ses.slug, cr.creative_name, p.publisher_name,
'none' as daypart, ses.spend,
ses.impressions
, install_lift_inc,
install_lift_1dvt,
install_lift_7dvt,
install_lift_28dvt
, uv_lift_inc,
uv_lift_1dvt,
uv_lift_7dvt,
uv_lift_28dvt
, df_metric_1_conversions_inc,
df_metric_1_conversions_1dvt,
df_metric_1_conversions_7dvt,
df_metric_1_conversions_28dvt
, df_metric_2_conversions_inc,
df_metric_2_conversions_1dvt,
df_metric_2_conversions_7dvt,
df_metric_2_conversions_28dvt
, df_metric_3_conversions_inc,
df_metric_3_conversions_1dvt,
df_metric_3_conversions_7dvt,
df_metric_3_conversions_28dvt
, df_metric_4_conversions_inc,
df_metric_4_conversions_1dvt,
df_metric_4_conversions_7dvt,
df_metric_4_conversions_28dvt
, df_metric_5_conversions_inc,
df_metric_5_conversions_1dvt,
df_metric_5_conversions_7dvt,
df_metric_5_conversions_28dvt
from ses
left join sales
on ses.campaign_id = sales.campaign_id
and ses.creative_id = sales.creative_id
and ses.view_date_et = sales.view_date_et
and ses.publisher_id = sales.publisher_id
and ses.client_id = sales.client_id
left join bi.client c on ses.client_id = c.id
left join bi.creative cr on ses.creative_id = cr.id
left join bi.publisher p on ses.publisher_id = p.id)
select *
from linear
where broadcast_week between '8/22/2022' and '8/23/2022'
union all
select *
from streaming
where broadcast_week between '8/22/2022' and '8/23/2022'
