USE CATALOG IDENTIFIER(:catalog);
USE IDENTIFIER(:schema);

with locations as (
  select id, explode(sensor_id_arr) as sensor_id, name
  from openaq_locations
  where
    city = :city
),
locations_sensors as (
  select
    locations.id as id,
    sensor_id,
    locations.name as name
  from locations
    join openaq_sensors on locations.sensor_id = openaq_sensors.id
    join openaq_parameters on openaq_sensors.parameter_id = openaq_parameters.id
  where
    openaq_parameters.name in ("pm10", "pm25", "co", "so2")
),
last_date as (
  select
    max(aqm.datetime_from) as last_date
  from openaq_measurements aqm
)
select
  id,
  name,
  date(last_date) as last_date,
  array_agg(sensor_id) as sensor_id_arr
from locations_sensors
  join last_date
group by id, name, last_date
