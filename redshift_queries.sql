-- creating the dimension table
create table if not exists airlines.airport(
    airport_id INTEGER,
    city VARCHAR(500),
    state VARCHAR(500),
    name VARCHAR(500)
);

-- loading data from s3 to the dimenion table
COPY airlines.airport
FROM 's3://airline-project-69/dim_data/airports.csv'
iam_role 'arn:aws:iam::992382771476:role/service-role/AmazonRedshift-CommandsAccessRole-20240701T204437'
delimiter ','
IGNOREHEADER 1;

select * from airlines.airport limit 5;


create table if not exists airlines.flight_dets(
    carrier varchar(20),
    depp_name VARCHAR(500),
    depp_city VARCHAR(500),
    depp_state VARCHAR(500),
    airr_name VARCHAR(500),
    airr_city VARCHAR(500),
    airr_state VARCHAR(500),
    depp_delay BIGINT,
    arr_delay BIGINT
);

select count(*) from airlines.flight_dets;


select * from airlines.flight_dets limit 10;

--creating view for Average Delays by Airline
CREATE MATERIALIZED VIEW airlines.avg_delays_by_airline AS
SELECT
  carrier,
  AVG(depp_delay) AS avg_depp_delay,
  AVG(arr_delay) AS avg_arr_delay
FROM airlines.flight_dets
GROUP BY carrier;

--Flights with High Delays

CREATE MATERIALIZED VIEW airlines.high_delays AS
SELECT *
FROM airlines.flight_dets
WHERE depp_delay > 120 OR arr_delay > 120;

--Delays by State

CREATE MATERIALIZED VIEW airlines.delays_by_state AS
SELECT
  depp_state,
  AVG(depp_delay) AS avg_depp_delay,
  airr_state,
  AVG(arr_delay) AS avg_arr_delay
FROM airlines.flight_dets
GROUP BY depp_state, airr_state;

-- High traffic cities

CREATE MATERIALIZED VIEW airlines.most_frequent_cities AS
SELECT
  depp_city,
  COUNT(*) AS total_departures,
  airr_city,
  COUNT(*) AS total_arrivals
FROM airlines.flight_dets
GROUP BY depp_city, airr_city;


-- On-Time Performance by Airline

CREATE MATERIALIZED VIEW airlines.on_time_performance AS
SELECT
  carrier,
  COUNT(*) AS total_flights,
  SUM(CASE WHEN depp_delay <= 0 AND arr_delay <= 0 THEN 1 ELSE 0 END) AS on_time_flights,
  (SUM(CASE WHEN depp_delay <= 0 AND arr_delay <= 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS on_time_percentage
FROM airlines.flight_dets
GROUP BY carrier;

-- Flight Count by Airport

CREATE MATERIALIZED VIEW airlines.flight_count_by_airport AS
SELECT
  depp_name AS airport,
  COUNT(*) AS total_departures
FROM airlines.flight_dets
GROUP BY depp_name
UNION ALL
SELECT
  airr_name AS airport,
  COUNT(*) AS total_arrivals
FROM airlines.flight_dets
GROUP BY airr_name;

