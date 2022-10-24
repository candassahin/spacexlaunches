--GET AVERAGE OF SUCCESFUL LAUNCHES BY MONTHS.

SELECT AVG(table1.amount) average_succesful_launch_amount_by_month FROM
(
SELECT
 	to_char(date_local, 'YYYY-MM'),
	COUNT(id) amount

FROM public.launches
GROUP BY to_char(date_local, 'YYYY-MM')
ORDER BY to_char(date_local, 'YYYY-MM')) table1 ;