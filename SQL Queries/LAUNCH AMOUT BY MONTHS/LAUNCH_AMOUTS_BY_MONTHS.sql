--LAUNCH AMOUTS BY MONTHS
SELECT
 	to_char(date_local, 'YYYY-MM'),
	COUNT(id) amount

FROM public.launches
GROUP BY to_char(date_local, 'YYYY-MM')
ORDER BY to_char(date_local, 'YYYY-MM');