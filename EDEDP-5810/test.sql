SELECT 
	t1.job_name, 
	t1.dag_id, 
	t2.dag_name, 
	t1.project, 
	t1.load_type, 
	t1.load_group,
	t1.start_time,
	t1.end_time,
	t1.run_id,
	t1.status
FROM public.job_log t1, public.dag_info t2
where lower(t1.project) like '%lighthouse%'
	and t1.dag_id = t2.dag_id
;
