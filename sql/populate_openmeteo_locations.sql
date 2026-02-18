USE CATALOG IDENTIFIER(:catalog);
USE IDENTIFIER(:schema);

create table if not exists dim_weather_locations (
  location_id string PRIMARY KEY,
  latitude float NOT NULL,
  longitude float NOT NULL,
  city string NOT NULL,
  timezone string,
  elevation float
);

merge into dim_weather_locations as target
using (
  select *
  from (
    select
      sha2(concat(latitude, "|", longitude), 256) as location_id,
      latitude,
      longitude,
      split(source_file_name, "_")[1] as city,
      timezone,
      elevation,
      row_number() over(partition by sha2(concat(latitude, "|", longitude), 256) order by bronze_load_ts desc) as row_num
    from air_polution_analytics_dev.`01_bronze`.temperature_measurements_raw
  ) where row_num = 1
) as source
on target.location_id = source.location_id
when not matched then insert *