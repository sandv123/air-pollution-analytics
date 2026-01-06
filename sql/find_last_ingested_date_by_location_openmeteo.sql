USE CATALOG IDENTIFIER(:catalog);
USE IDENTIFIER(:schema);

select
  date(max(t.datetime)) as last_date,
  l.city,
  l.latitude,
  l.longitude
from temperature_measurements t
  join temperature_locations l on t.location_id = l.location_id
-- where city = :city
group by l.city, l.latitude, l.longitude