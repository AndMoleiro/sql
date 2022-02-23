/////////QA TRAINING

-- S163 - Healthgrades (Parent: s068)

//////STEP 2 -> STEP 3

/*

Step 2 data:

-- Physicians:

    S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s068"
    S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s100"
    S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"

-- Group Directory

    S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s202"


Step 3 data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p03/s068"

*/

/*
 +------+------------+-----------+---------+
|SOURCE|STEP_2_COUNT|STEP_3_COUNT|DIFF     |
+------+------------+------------+---------+
|s068  | 808254     | 808254     | 0       |
|s100  | 859796     | 859796     | 0       |
|s163  | 868895     | 868895     | 0       |
|s202  | 320461     | 422157     | -101696 |
+------+------------+------------+---------+
 */

with
	step_2 as (
				  select
					  's202'   source,
					  count(*) count
				  from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s202"
				  union all
				  select
					  's163'   source,
					  count(*) count
				  from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
				  union all
				  select
					  's100'   source,
					  count(*) count
				  from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s100"
	              union all
				  select
					  's068'   source,
					  count(*) count
				  from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s068"
	),
	step_3 as (
				  select
					  array_to_string(source_indexes, ',') source,
					  count(*)                             count
				  from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p03/s068"
				  group by 1
	)
select
	step_2.source                   as  source,
	step_2.count                    as  step_2_count,
	step_3.count                    as  step_3_count,
	step_2.count - step_3.count     as diff
from step_2
	 left join step_3
			  on step_2.source = step_3.source;



////ENTITY TO ENTITY

with
     step2 as (
                select
                    source_indexes,
                    url
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s068"
                union
                select
                    source_indexes,
                    url
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s100"
                union
                select
                source_indexes,
                url
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
                union
                select
                source_indexes,
                url
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s202"
     )
select
    count(step3.url),
    count(step2.url)
from  step2
    left join S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p03/s068" as step3
        on step2.url = step3.url and step2.source_indexes = step3.source_indexes

-- COUNT(STEP3.URL): 2959102
-- COUNT(STEP2.URL): 2959102



////COUNT COMPARISON

with
    step2 as (
                select
                    source_indexes,
                    count(url) as total
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s068"
                group by 1
                union
                select
                    source_indexes,
                    count(url) as total
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s100"
                group by 1
                union
                select
                    source_indexes,
                    count(url) as total
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
                group by 1
                union
                select
                    source_indexes,
                    count(url) as total
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s202"
                group by 1
    ),

    step3 as (
                select
                    source_indexes,
                    count(url) as total
                from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p03/s068"
                group by 1
    )

select
    step2.source_indexes,
    step2.total,
    step3.total,
    step2.total - step3.total as diff
from  step2
    left join  step3
        on step2.source_indexes = step3.source_indexes



-- I understand the concept of exploding the locations_struct array but I don't understand how the following query accounts for that


select 's202'   source,
           (count(*) + (
               select count(*) from "S3_DATA_SOURCES".CARPE_DATASTORE_COMMERCIAL."p02/s202"
               where LOCATIONS_STRUCT is null
               )) as count
    from "S3_DATA_SOURCES".CARPE_DATASTORE_COMMERCIAL."p02/s202",
         lateral flatten(locations_struct) as f