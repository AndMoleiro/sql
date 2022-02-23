/////////QA TRAINING

-- S163 - Healthgrades (Parent: s068)

//////STEP 4 -> STEP 5

/*

Step 4 data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"

Step 5 data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068"

*/

//////OVERALL COUNTS OF SOURCES

/*

+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
| 2959102  |2959102 |    0 |
+----------+--------+------+

*/

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068"
    group by 1
)

select
    input_tab.t as expected,
    output_tab.t as actual,
    expected - actual as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;


//////FIELD COMPARISON

/*
+----------------------------+----------------------+---------------------------------------------------------+
| business_transformer_field | transformer_field    |                       pseudocode                        |
+----------------------------+----------------------+---------------------------------------------------------+
| business_tags.tags.name    | accepts_new_patients | case(TRUE: “Accepts New Patients”).fill(“value”: “Yes”) |
| business_tags.tags.value   |                      |                                                         |
+----------------------------+----------------------+---------------------------------------------------------+


+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|   245193 | 245193 |    0 |
+----------+--------+------+
*/

//COUNTS

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
    where accepts_new_patients is not null
    group by 1
),

output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
        lateral flatten(business_tags:tags) as f
    where f.value:name = 'Accepts New Patients'
    group by 1
)

select
    input_tab.t as expected,
    output_tab.t as actual,
    expected - actual as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                                         as id,
        tools.public.array_sort(array_agg(regexp_replace(
            accepts_new_patients, true, 'Accepts New Patients') || ' ' || 'Yes'))           as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
    group by 1
),

output_tab as (
    select
        reshaped_id                                                                         as id,
        tools.public.array_sort(array_agg(
            f.value:name || ' ' || f.value:value))                                          as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(business_tags:tags)                                                as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
    array_intersection(output_tab.dp, input_tab.dp)                         as intersection,
    input_tab.dp = intersection                                             as intersected
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where array_size(input_tab.dp) >= 1
    and intersected = false;



--------------------------------------------------------------------------------------------------------------

/*
+------------------------------+-------------------------------+------------+
|  business_transformer_field  |       transformer_field       | pseudocode |
+------------------------------+-------------------------------+------------+
| affiliated_hospitals[#].name | business.associations[#].name | no-op      |
+------------------------------+-------------------------------+------------+


+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|   395878 | 395878 |    0 |
+----------+--------+------+
*/

//COUNTS

with input_tab as (
    select
        ' '             as joining,
        count(*)        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(affiliated_hospitals) as f
    where f.value:name is not null
    group by 1
),

output_tab as (
    select
        ' '             as joining,
        count(*)        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
        lateral flatten(business:associations) as f
    where f.value:name is not null
    group by 1
)

select
    input_tab.t         as expected,
    output_tab.t        as actual,
    expected - actual   as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                 as id,
        f.value:name                as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
         lateral flatten(affiliated_hospitals) as f
),

output_tab as (
    select
        reshaped_id                 as id,
            business:associations   as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068"
)

select
    input_tab.*,
    output_tab.*
from input_tab
    left join output_tab
        on input_tab.id = output_tab.id
where input_tab.dp is not null;


--------------------------------------------------------------------------------------------------------------

/*
+----------------------------------------+---------------------------------------+---------------------------------+
|       business_transformer_field       |           transformer_field           |           pseudocode            |
+----------------------------------------+---------------------------------------+---------------------------------+
| classification.categories[#].category  | practice_areas[#].clinical_focus_name | fill("relevance": "additional") |
| classification.categories[#].relevance |                                       |                                 |
+----------------------------------------+---------------------------------------+---------------------------------+



+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|   68534  | 68534  |    0 |
+----------+--------+------+
*/

//COUNTS

with input_tab as (
    select
        reshaped_id                                                 as id,
        tools.public.array_sort(array_agg(
            f.value:clinical_focus_name))                           as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(practice_areas) as f
    group by 1
),

output_tab as (
    select
        reshaped_id                                                 as id,
        tools.public.array_sort(array_agg(
            f.value:category))                                      as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(classification:categories) as f
    group by 1
)

select
    count(input_tab.id)                                             as expected,
    count(array_intersection(output_tab.dp, input_tab.dp))          as actual,
    actual - expected                                               as diff
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                     as id,
        tools.public.array_sort(array_agg(
            f.value:clinical_focus_name))                               as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(practice_areas) as f
    group by 1
),

output_tab as (
    select
        reshaped_id                                                     as id,
        tools.public.array_sort(array_agg(
            f.value:category))                                          as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(classification:categories) as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
    array_intersection(output_tab.dp, input_tab.dp)                     as intersection,
    input_tab.dp = intersection                                         as intersected
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where array_size(input_tab.dp) >= 1
    and intersected = false;


------------------------------------------------------------------------------------------------------------------------

/*
+-----------------------------+-------------------+-------------------------------------------------+
| business_transformer_field  | transformer_field |                   pseudocode                    |
+-----------------------------+-------------------+-------------------------------------------------+
| business_tags.tags[#].name  | telehealth_text   | case(_: “telehealth text”).fill(“value”: “Yes”) |
| business_tags.tags[#].value |                   |                                                 |
+-----------------------------+-------------------+-------------------------------------------------+

+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|  127863  | 127863 |    0 |
+----------+--------+------+
*/

select
    f.value:name            as name,
    count(f.value:name)     as t
from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
    lateral flatten(business_tags:tags) as f
where contains(lower(f.value:name), 'telehealth')
group by 1;

-- "Telehealth": 50
-- "Telehealth services available": 127813

        ---> Logic is not implemented correctly. All results should be "telehealth text"


//COUNTS

with input_tab as (
    select
        ' '                 as joining,
        count(*)            as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
        where telehealth_text is not null
    group by 1
),

output_tab as (
    select
        ' '                 as joining,
        count(*)            as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
        lateral flatten(business_tags:tags) as f
    where   f.value:name = 'Telehealth'
        or  f.value:name = 'Telehealth services available'
    group by 1
)

select
    input_tab.t             as expected,
    output_tab.t            as actual,
    expected - actual       as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                     as id,
        tools.public.array_sort(array_agg(
            telehealth_text || ' ' || 'Yes'))                           as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068"
    group by 1
),

output_tab as (
    select
        reshaped_id                                                     as id,
        tools.public.array_sort(array_agg(
            f.value:name || ' ' || f.value:value))                      as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(business_tags:tags) as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
array_intersection(output_tab.dp, input_tab.dp)                         as intersection,
input_tab.dp = intersection                                             as intersected
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where array_size(input_tab.dp) >= 1
    and intersected = false;


--------------------------------------------------------------------------------------------------------------

/*
+----------------------------------------+-------------------------------------+---------------------------------+
|       business_transformer_field       |          transformer_field          |           pseudocode            |
+----------------------------------------+-------------------------------------+---------------------------------+
| classification.categories[#].category  | practice_areas[#].dcp_items[#].name | fill("relevance": "additional") |
| classification.categories[#].relevance |                                     |                                 |
+----------------------------------------+-------------------------------------+---------------------------------+

+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|   68534  | 68534  |    0 |
+----------+--------+------+
*/

//COUNTS

with input_tab as (
    select
        reshaped_id                                                     as id,
        tools.public.array_sort(array_agg(
            f2.value:name))                                             as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(practice_areas)     as f,
        lateral flatten(f.value:dcp_items)  as f2
group by 1
),

output_tab as (
    select
    reshaped_id                                                         as id,
    tools.public.array_sort(array_agg(
        f.value:category))                                              as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(classification:categories) as f
    group by 1
)

select
    count(input_tab.id)                                                 as expected,
    count(array_intersection(output_tab.dp, input_tab.dp))              as actual,
    actual - expected                                                   as diff
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                     as id,
        tools.public.array_sort(array_agg(
            f2.value:name || ' ' || 'additional'))                      as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(practice_areas)     as f,
        lateral flatten(f.value:dcp_items)  as f2
    group by 1
),
output_tab as (
    select
        reshaped_id                                                     as id,
        tools.public.array_sort(array_agg(
            f.value:category || ' ' || f.value:relevance))              as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(classification:categories) as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
array_intersection(output_tab.dp, input_tab.dp)                         as intersection,
input_tab.dp = intersection                                             as intersected
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where array_size(input_tab.dp) >= 1
    and intersected = false;

------------------------------------------------------------------------------------------------------------------------

/*
+------------------------------------------+---------------------------------------+-----------------------------------------+
|        business_transformer_field        |           transformer_field           |               pseudocode                |
+------------------------------------------+---------------------------------------+-----------------------------------------+
| business.associations[#].association_url | affiliated_hospitals[#].main_site_url | format(“https://www.healthgrades.com$”) |
+------------------------------------------+---------------------------------------+-----------------------------------------+

+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|   395766 | 395766 |    0 |
+----------+--------+------+
*/

//COUNTS

with input_tab as (
    select
        ' '             as joining,
        count(*)        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
         lateral flatten(affiliated_hospitals) as f
    where f.value:main_site_url is not null
    group by 1

),

output_tab as (
    select
        ' '             as joining,
        count(*)        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
        lateral flatten(business:associations) as f
    where f.value:association_url is not null
)

select
    input_tab.t         as expected,
    output_tab.t        as actual,
    expected - actual   as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;

//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                             as id,
        tools.public.array_sort(array_agg(
            'https://www.healthgrades.com' || '' || f.value:main_site_url ))    as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(affiliated_hospitals) as f
    group by 1
),

output_tab as (
    select
    reshaped_id                                                                 as id,
        tools.public.array_sort(array_agg(
            f.value:association_url))                                           as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(business:associations) as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
    input_tab.dp = output_tab.dp    as transformed
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where transformed = false;


------------------------------------------------------------------------------------------------------------------------

/*
+------------------------------------+----------------------------------------------------------+-----------------------------+
|     business_transformer_field     |                    transformer_field                     |         pseudocode          |
+------------------------------------+----------------------------------------------------------+-----------------------------+
| practice_locations[#].full_address | ($1) office_locations[#].office_locations[#].street      | format (“$1, $2, $3 $4 $5”) |
|                                    | ($2) office_locations[#].office_locations[#].city        |                             |
|                                    | ($3) office_locations[#].office_locations[#].state       |                             |
|                                    | ($4) office_locations[#].office_locations[#].postal_code |                             |
|                                    | ($5) office_locations[#].office_locations[#].nation      |                             |
+------------------------------------+----------------------------------------------------------+-----------------------------+

+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|  1032476 | 1032476|    0 |
+----------+--------+------+
*/


---> Logic is not implemented correctly:

    -- "8970 Market St, Dover, AR 72837 USA" -> "8970 Market St,Dover,AR 72837 USA"

        -- Sould be format (“$1, $2, $3 $4 $5”), is instead format (“$1,$2,$3 $4 $5”)



//COUNTS

with input_tab as (
    select
        ' '                                                             as joining,
        count(*)                                                        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
         lateral flatten(office_locations)                              as f,
         lateral flatten(f.value:office_locations)                      as f2
    where
            f2.value:street         is not null
        or  f2.value:city           is not null
        or  f2.value:state          is not null
        or  f2.value:postal_code    is not null
        or  f2.value:nation         is not null

),

output_tab as (
    select
        ' '                                                             as joining,
        count(*)                                                        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
        lateral flatten(practice_locations)                             as f
    where f.value:full_address is not null
)

select
    input_tab.t                                                         as expected,
    output_tab.t                                                        as actual,
    expected - actual                                                   as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                     as id,
        tools.public.array_sort(array_agg(
            f2.value:street         || ',' ||
            f2.value:city           || ',' ||
            f2.value:state          || ' ' ||
            f2.value:postal_code    || ' ' ||
            f2.value:nation))                                           as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
         lateral flatten(office_locations)                              as f,
         lateral flatten(f.value:office_locations)                      as f2
    group by 1
),

output_tab as (
    select
        reshaped_id                                                     as id,
         tools.public.array_sort(array_agg(
             f.value:full_address))                                     as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(practice_locations)                            as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
    input_tab.dp = output_tab.dp                                        as transformed
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where transformed = false;



------------------------------------------------------------------------------------------------------------------------

/*
+-----------------------------------+------------------------------------+-----------------------------------------+
|    business_transformer_field     |         transformer_field          |               pseudocode                |
+-----------------------------------+------------------------------------+-----------------------------------------+
| practice_locations[#].location_url| office_locations[#].               |                                         |
|                                   | office_locations[#].main_site_url  | format(“https://www.healthgrades.com$”) |
+-----------------------------------+------------------------------------+-----------------------------------------+

+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|  449957  | 449957 |    0 |
+----------+--------+------+
*/


//COUNTS

with input_tab as (
    select
        ' '                                                             as joining,
        count(*)                                                        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
         lateral flatten(office_locations)                              as f,
         lateral flatten(f.value:office_locations)                      as f2
    where f2.value:main_site_url is not null
    group by 1

),

output_tab as (
    select
        ' '             as joining,
        count(*)        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
        lateral flatten(practice_locations)                             as f
    where f.value:location_url is not null
)

select
    input_tab.t                                                         as expected,
    output_tab.t                                                        as actual,
    expected - actual                                                   as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                             as id,
        tools.public.array_sort(array_agg(
            'https://www.healthgrades.com' || '' || f2.value:main_site_url ))   as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(office_locations)                                       as f,
        lateral flatten(f.value:office_locations)                               as f2
    group by 1
),

output_tab as (
    select
    reshaped_id                                                                 as id,
        tools.public.array_sort(array_agg(
            f.value:location_url))                                              as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(practice_locations)                                    as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
    input_tab.dp = output_tab.dp                                                as transformed
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where array_size(input_tab.dp) >= 1
    and transformed = false;



------------------------------------------------------------------------------------------------------------------------

/*
+-------------------------------------+----------------------------------------------+------------+
|     business_transformer_field      |              transformer_field               | pseudocode |
+-------------------------------------+----------------------------------------------+------------+
| practice_locations[#].location_name | office_locations[#].office_locations[#].name | no-op      |
+-------------------------------------+----------------------------------------------+------------+

+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|  769496  | 769496 |    0 |
+----------+--------+------+
*/


//COUNTS

with input_tab as (
    select
        ' '                                                             as joining,
        count(*)                                                        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
         lateral flatten(office_locations)                              as f,
         lateral flatten(f.value:office_locations)                      as f2
    where f2.value:name is not null
    group by 1

),

output_tab as (
    select
        ' '             as joining,
        count(*)        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
        lateral flatten(practice_locations)                             as f
    where f.value:location_name is not null
)

select
    input_tab.t                                                         as expected,
    output_tab.t                                                        as actual,
    expected - actual                                                   as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                             as id,
        tools.public.array_sort(array_agg(
            f2.value:name ))                                                    as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(office_locations)                                       as f,
        lateral flatten(f.value:office_locations)                               as f2
    group by 1
),

output_tab as (
    select
    reshaped_id                                                                 as id,
        tools.public.array_sort(array_agg(
            f.value:location_name))                                             as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(practice_locations)                                    as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
    input_tab.dp = output_tab.dp                                                as transformed
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where array_size(input_tab.dp) >= 1
    and transformed = false;



------------------------------------------------------------------------------------------------------------------------

/*
+-------------------------------------+-----------------------------------+------------+
|     business_transformer_field      |         transformer_field         | pseudocode |
+-------------------------------------+-----------------------------------+------------+
| practice_locations[#].practice_name | office_locations[#].practice_name | no-op      |
+-------------------------------------+-----------------------------------+------------+

+----------+--------+------+
| expected | actual | diff |
+----------+--------+------+
|  945964  | 1032468|-86504|
+----------+--------+------+
*/


//COUNTS

with input_tab as (
    select
        ' '                                                             as joining,
        count(*)                                                        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
         lateral flatten(office_locations)                              as f
    where f.value:practice_name is not null
    group by 1

),

output_tab as (
    select
        ' '                                                             as joining,
        count(*)                                                        as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
        lateral flatten(practice_locations)                             as f
    where f.value:practice_name is not null
)

select
    input_tab.t                                                         as expected,
    output_tab.t                                                        as actual,
    expected - actual                                                   as diff
from input_tab
    left join output_tab
        on input_tab.joining = output_tab.joining;


//ENTITY TO ENTITY


with input_tab as (
    select
        reshaped_id                                                             as id,
        tools.public.array_sort(array_agg(
            f.value:practice_name ))                                            as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
        lateral flatten(office_locations)                                       as f
    group by 1
),

output_tab as (
    select
        reshaped_id                                                             as id,
        tools.public.array_sort(array_agg(
            f.value:practice_name))                                             as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
         lateral flatten(practice_locations)                                    as f
    group by 1
)

select
    input_tab.*,
    output_tab.*,
    input_tab.dp = output_tab.dp                                                as transformed
from input_tab
    full outer join output_tab
        on input_tab.id = output_tab.id
where array_size(input_tab.dp) >= 1
    and transformed = false;


-- Checking if the number of office_locations[#].office_locations[#].city (100% fill rate) in step 4 would be the same
-- as practice_locations[#].practice_name.

        -- step4_cities:            1032476
        -- tep5_practice_names:     1032468


select
    count(f1.value:city)                                                as step4_cities,

    (   select
            count(f.value:practice_name)
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
            lateral flatten(practice_locations)                         as f
        )                                                               as step5_practice_names

from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
    lateral flatten(office_locations)                                   as f,
    lateral flatten(f.value:office_locations)                           as f1;


-- Checking for the 8 different records:


with input_tab as (
        select
            reshaped_id                                                 as id,
            array_agg(f.value)                                          as office_locations,
            count(f1.value:city)                                        as t
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p04/s068",
            lateral flatten(office_locations) as f,
            lateral flatten(f.value:office_locations) as f1
        group by 1

    ),

     output_tab as(
         select
            reshaped_id                                                 as id,
            array_agg(f.value)                                          as practice_locations,
            count(f.value:practice_name)                                as t
        from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p05/s068",
            lateral flatten(practice_locations)                         as f
        group by 1

    )

select
    input_tab.*,
    output_tab.*
from input_tab
    join output_tab
        on input_tab.id = output_tab.id
where input_tab.t = output_tab.t;

