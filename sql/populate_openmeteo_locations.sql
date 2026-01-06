USE CATALOG IDENTIFIER(:catalog);
USE IDENTIFIER(:schema);

create table if not exists temperature_locations (
  location_id string,
  latitude float,
  longitude float,
  city string,
  timezone string,
  elevation float
);

merge into temperature_locations as target
using (
  select
    distinct sha2(concat(latitude, "|", longitude), 256) as location_id,
    latitude,
    longitude,
    split(source_file_name, "_")[1] as city,
    timezone,
    elevation
  from air_polution_analytics_dev.`01_bronze`.temperature_measurements
) as source
on target.location_id = source.location_id
when not matched then insert *