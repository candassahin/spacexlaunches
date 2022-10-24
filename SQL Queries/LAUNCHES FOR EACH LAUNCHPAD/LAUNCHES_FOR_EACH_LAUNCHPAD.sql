--LAUNCHES FOR EACH LAUNCHPAD
SELECT
	lp.launchpad_service_id,
	COUNT(launches.id)
FROM public.launches
INNER JOIN launchpads lp on lp.launchpad_service_id = launches.launchpad
GROUP BY lp.launchpad_service_id;