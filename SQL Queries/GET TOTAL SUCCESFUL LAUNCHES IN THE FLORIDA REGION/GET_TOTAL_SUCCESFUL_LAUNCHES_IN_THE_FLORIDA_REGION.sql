--GET TOTAL SUCCESFUL LAUNCHES IN THE FLORIDA REGION
SELECT 
	COUNT(launches.id) as launche_amount
FROM public.launches
INNER JOIN launchpads lp on lp.launchpad_service_id = launches.launchpad
WHERE success IS TRUE AND lp.region = 'Florida';