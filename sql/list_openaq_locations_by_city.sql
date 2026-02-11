USE CATALOG IDENTIFIER(:catalog);
USE IDENTIFIER(:schema);

select
  openaq_locations.id as id,
  array_agg(openaq_sensors.id) as sensor_id_arr,
  openaq_locations.name as name
from openaq_locations
  join openaq_sensors on openaq_locations.id = openaq_sensors.location_id
  join openaq_parameters on openaq_sensors.parameter_id = openaq_parameters.id
where
  openaq_parameters.name in ("pm10", "pm25", "co", "so2") and
  openaq_locations.city = :city
group by openaq_locations.id, openaq_locations.name