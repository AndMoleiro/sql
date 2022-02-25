/*

Returns the number of records that satisfy a condition.

+------------------------------------------------+
|                     syntax                     |
+------------------------------------------------+
|               count_if( <condition> )          |
+------------------------------------------------+

*/

-- Eg 1: using count_if as a multiple case statement

select
reshaped_id as id,
count_if(
    lower(f.value) like '%has closed%'
    or closed = true
    or (lower(name) like '%closed%'
    and lower(name) not like '%temporarily%'
        and lower(name) not like '%seasonally%')) > 0 as dp
from S3_DATA_SOURCES.CARPE_DATASTORE_QA."MDV/4748/02-01-2022/p04/s010",
 lateral flatten(alert, outer => true) as f
group by 1
