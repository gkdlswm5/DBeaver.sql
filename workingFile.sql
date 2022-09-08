select *
from core.spot_level_metrics slm
limit 200

select
    'linear' as type,
    slm.spot_id as spot_id,
    slm.
from core.spot_level_metrics slm
left join philo.spots s on slm.spot_id = s.id

select *
from philo.spots s
limit 30
