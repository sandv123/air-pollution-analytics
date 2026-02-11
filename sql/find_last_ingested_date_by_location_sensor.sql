USE CATALOG IDENTIFIER(:catalog);
USE IDENTIFIER(:schema);

with locations_sensors as (
  select
    openaq_locations.id as id,
    openaq_sensors.id as sensor_id,
    openaq_locations.name as name
  from openaq_locations
    join openaq_sensors on openaq_locations.id = openaq_sensors.location_id
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
