WITH departures AS (
	SELECT origin AS airport_code, 
			COUNT(DISTINCT dest) AS nunique_to, -- unique number of departures connections
			COUNT(sched_dep_time) AS dep_planned, -- how many departure flights were planned
			SUM(cancelled) AS dep_cancelled, -- how many departure flights were cancelled
			SUM(diverted) AS dep_diverted, -- how many departure flights were diverted
			COUNT(dep_time) AS dep_n_flights -- how many departure flights actually happened
	FROM {{ref('prep_flights')}}
	GROUP BY origin
),
arrivals AS (
	SELECT dest AS airport_code, 
			COUNT(DISTINCT origin) AS nunique_from, -- unique number of arrival connections
			COUNT(sched_arr_time) AS arr_planned, -- how many arrival flights were planned
			SUM(cancelled) AS arr_cancelled, -- how many arrival flights were cancelled
			SUM(diverted) AS arr_diverted, -- how many arrival flights were diverted
			COUNT(arr_time) AS arr_n_flights -- how many arrival flights actually happened
	FROM {{ref('prep_flights')}}
	GROUP BY dest
),
total_stats AS (
	SELECT d.airport_code,
			nunique_to, 
			nunique_from,
			(dep_planned + arr_planned) AS total_planned,
			(dep_cancelled + arr_cancelled) AS total_cancelled,
			(dep_diverted + arr_diverted) AS total_diverted,
			(dep_n_flights + arr_n_flights) AS total_flights
	FROM departures d
	JOIN arrivals a
	-- ON d.airport_code = a.airport_code
	USING (airport_code)
)
SELECT a.city, a.country, a.name,
		ts.* 
FROM total_stats ts
JOIN {{ref('prep_airports')}} a
ON ts.airport_code = a.faa