/////////QA TRAINING

-- S163 - Healthgrades (Parent: s068)

//////STEP 3 -> STEP 4

/*

Step 3 data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p03/s068"

Step 4 data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"

*/

select * from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"

-- Determine how many R01, R01N, A01...

select f.value:parser_name                   as pname,
       count(*)                              as t,
       count(*)::double /
       (select count(*) from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"):: double as perc
from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
     lateral flatten(INTERNAL_ADDRESS_DATA:addresses) as f
group by 1
order by 1

/*

+--------+---------+---------------------+
| pname  |    t    |        perc         |
+--------+---------+---------------------+
| "A03"  |  187090 |   06322526225861765 |
| "A03M" |  187090 | 0.06322526225861765 |
| "A01"  | 2938964 |  0.9931945569973594 |
| "A01M" | 2938964 |  0.9931945569973594 |
| "R01"  | 2959102 |                   1 |
| "R01N" | 2959102 |                   1 |
| "A02"  | 3899831 |  1.3179102984621687 |
| "A02M" | 3899831 |  1.3179102984621687 |
+--------+---------+---------------------+

-- R01/R01N ->  (RAW/RAW Normalized) Return on the records provided -> No added information

-- A01/A01M ->  Internal Parser -> Parses the data -> No added information

-- A02/AO2M ->  SmartyStreets   -> Adds more information (Eg: coordinates)

-- AO3/AO3M ->  Google maps     -> Adds more information (Eg: coordinates)


*/

////ENTITY TO ENTITY

//city

with input_tab as (
        select
            reshaped_id,
            city as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
),

output_tab as (
        select
            reshaped_id,
            f.value:city as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
            ,lateral flatten(internal_address_data:addresses) as f
        where f.value:parser_name = 'A01'
)

select
    input_tab.*,
    output_tab.*
from input_tab
    left join output_tab
        on input_tab.reshaped_id = output_tab.reshaped_id
where input_tab.dp <> output_tab.dp
--where lower(input_tab.dp) <> lower(output_tab.dp)


--

with input_tab as (
        select
            reshaped_id,
            city as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
),

output_tab as (
        select
            reshaped_id,
            f.value:city as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
            ,lateral flatten(internal_address_data:addresses) as f
        where f.value:parser_name = 'A01'
)

select
    input_tab.dp,
    output_tab.dp,
    count(*)
from input_tab
    left join output_tab
        on input_tab.reshaped_id = output_tab.reshaped_id
group by 1,2
order by 3 desc


with input_tab as (
        select
            reshaped_id,
            city as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
),

output_tab as (
        select
            reshaped_id,
            f.value:city as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
            ,lateral flatten(internal_address_data:addresses) as f
        where f.value:parser_name = 'A01'
)

select
    input_tab.*,
    output_tab.*
from input_tab
    left join output_tab
        on input_tab.reshaped_id = output_tab.reshaped_id
where input_tab.dp is null and output_tab.dp = 'New York'


-- 61673d9fbd0673be

select
    state,
    city,
    street,
    zip,
    internal_address_data
from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
where reshaped_id = '61673d9fbd0673be'

-- The parsers returns values from null values

//state

with input_tab as (
        select
            reshaped_id,
            state as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
),

output_tab as (
        select
            reshaped_id,
            f.value:state as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
            ,lateral flatten(internal_address_data:addresses) as f
        where f.value:parser_name = 'A01'
)

select
    input_tab.*,
    output_tab.*
from input_tab
    left join output_tab
        on input_tab.reshaped_id = output_tab.reshaped_id
where input_tab.dp <> output_tab.dp



select
    state,
    city,
    street,
    zip,
    internal_address_data
from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
where reshaped_id = 'b4de0b8c5cf6631d'

-- The address parser ('R01' to 'R01N') is wrongly changing the state from VA to DC

--

with input_tab as (
        select
            reshaped_id,
            state as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
),

output_tab as (
        select
            reshaped_id,
            f.value:state as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
            ,lateral flatten(internal_address_data:addresses) as f
        where f.value:parser_name = 'A01'
)

select
    input_tab.dp,
    output_tab.dp,
    count(*)
from input_tab
    left join output_tab
        on input_tab.reshaped_id = output_tab.reshaped_id
group by 1,2
order by 3 desc


//street_address

with input_tab as (
        select
            reshaped_id,
            street as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
),

output_tab as (
        select
            reshaped_id,
            f.value:street as dp,
            f.value as iad
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
            ,lateral flatten(internal_address_data:addresses) as f
        where f.value:parser_name = 'A01'
)

select
    --input_tab.*,
    --output_tab.*
    input_tab.dp,
    output_tab.dp,
    output_tab.iad
from input_tab
    left join output_tab
        on input_tab.reshaped_id = output_tab.reshaped_id
where input_tab.dp <> output_tab.dp


//state

with input_tab as (
        select
            reshaped_id,
            zip as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
),

output_tab as (
        select
            reshaped_id,
            f.value:zip as dp
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
            ,lateral flatten(internal_address_data:addresses) as f
        where f.value:parser_name = 'A01'
)

select
    input_tab.*,
    output_tab.*
from input_tab
    left join output_tab
        on input_tab.reshaped_id = output_tab.reshaped_id
where input_tab.dp <> output_tab.dp





select
*
from
(select
RESHAPED_ID, count(distinct f.VALUE:parser_name) as dist, count(f.VALUE:parser_name) as c
from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
     lateral flatten(INTERNAL_ADDRESS_DATA:addresses) as f
where RESHAPED_ID not in (
    select RESHAPED_ID
    from
    (select
       RESHAPED_ID, count(*) as t
from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
group by 1
having t > 1))
group by 1)
where c > 8;