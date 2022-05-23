SELECT
	--1--
	time_bucket('1 min', main.time) AS tt,
	--2--
	COUNT(*),
	COUNT(*) / 60 as speed
FROM audit.main
WHERE main.time >= '2021-02-01'
GROUP BY tt
ORDER BY tt;

SELECT pg_size_pretty(pg_total_relation_size('"audit"."main"'));


select count(*) from audit.main

select time from audit.main
limit 100

select id, trace_id  from audit.component;

select *
from audit.error_metrics;
