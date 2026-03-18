with today_dag as (
select dag_id, next_dagrun_data_interval_start, next_dagrun_data_interval_end
from dag 
where is_paused = false
--and timetable_summary  not in ('null')
--and (date(next_dagrun_data_interval_start) between current_date -1 and current_date
--or date(next_dagrun_data_interval_end) between current_date -1 and current_date)
)
, today_dagrun as (
select dag_id, 
count(1) as run_cnt
, count(case when state = 'success' then 'success' end) as success_cnt
, count(case when state = 'failed' then 'failed' end) as failed_cnt
from dag_run
where date(data_interval_end) between current_date -1 and current_date
group by 1
)
select AA.dag_id, BB.*
from today_dag as AA
left join today_dagrun as BB
on AA.dag_id = BB.dag_id
