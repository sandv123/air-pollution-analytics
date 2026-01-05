USE CATALOG IDENTIFIER(:catalog);
USE IDENTIFIER(:schema);

with locations as (
  select id, explode(sensor_id_arr) as sensor_id, name
  from openaq_locations
  where
    city = :city
)
select
  locations.id as id,
  array_agg(sensor_id) as sensor_id_arr,
  locations.name as name
from locations
  join openaq_sensors on locations.sensor_id = openaq_sensors.id
  join openaq_parameters on openaq_sensors.parameter_id = openaq_parameters.id
where
  openaq_parameters.name in ("pm10", "pm25", "co", "so2")
group by locations.id, locations.name