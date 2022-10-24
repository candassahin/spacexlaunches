--DATA LAUNCHES FOR A MONTH.(GETS LAST MONTHS DATA )

SELECT
	id,
	launch_service_id,
	static_fire_date_utc,
	static_fire_date_unix,
	net,
	"window",
	rocket,
	success,
	details,
	launchpad,
	flight_number,
	"name",
	date_utc,
	date_unix,
	date_local,
	date_precision,
	upcoming,
	auto_update,
	tbd,
	launch_library_id
FROM public.launches
WHERE date_local >= date_trunc('month', current_date - interval '1' month) and 
	date_local  < date_trunc('month', current_date)
ORDER BY date_local;