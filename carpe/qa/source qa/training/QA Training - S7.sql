/////////QA TRAINING

-- S163 - Healthgrades (Parent: s068)

//////STEP 7

/*


Step 7g data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p07/g_base"

Step 7h data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p07/h_base"

*/

//////STEP 7g

////OVERALL COUNTS OF SOURCES

//SNOWFLAKE

/*
+------+----------+----------+------+
| SID  | 7a_total | 7g_total | DIFF |
+------+----------+----------+------+
| s202 |   467961 |   467527 |  434 |
| s100 |   952775 |   951810 |  965 |
| s068 |   895244 |   894306 |  938 |
| s163 |   962168 |   961166 | 1002 |
+------+----------+----------+------+
*/

with step7a as (
    select
        f.value  as sid,
        count(*) as total
    from "S3_DATA_SOURCES"."CARPE_DATASTORE_COMMERCIAL"."p07/a_base",
         lateral flatten(SOURCES:source_ids) as f
    where contains(SOURCES:unique_source_ids, 's068')
    group by 1
),
     step7g as (
         select
            f.value  as sid,
            count(*) as total
         from "S3_DATA_SOURCES"."CARPE_DATASTORE_COMMERCIAL"."p07/g_base",
              lateral flatten(SOURCES:source_ids) as f
         where contains(SOURCES:unique_source_ids, 's068')
         group by 1
     )

select step7a.sid,
       step7a.total                 as "7a_total",
       step7g.total                 as "7g_total",
       step7a.total - step7g.total  as diff
from step7a
         join step7g
              on step7a.sid = step7g.sid


//ATHENA

/*
+------+----------+----------+------+
| SID  | 7a_total | 7g_total | DIFF |
+------+----------+----------+------+
| s202 |   467961 |   467961 |   0  |
| s100 |   952775 |   952775 |   0  |
| s068 |   895244 |   895244 |   0  |
| s163 |   962168 |   962168 |   0  |
+------+----------+----------+------+
*/

with step7a as (
    select
        f  as sid,
        count(*) as total
    from "p07_a",
         unnest(SOURCES.source_ids) as t(f)
    where contains(SOURCES.unique_source_ids, 's068')
    group by 1
),

    step7g as (
     select
        f  as sid,
        count(*) as total
     from "p07_g",
          unnest(SOURCES.source_ids) as t(f)
     where contains(SOURCES.unique_source_ids, 's068')
     group by 1
     )

select step7a.sid,
        step7a.total                    as "7a_total",
        step7g.total                    as "7g_total"
        step7a.total - step7g.total     as diff
from step7a
         join step7g
              on step7a.sid = step7g.sid



//////STEP 7g

////OVERALL COUNTS OF SOURCES

//SNOWFLAKE

/*
+------+----------+----------+------+
| SID  | 7g_total | 7h_total | DIFF |
+------+----------+----------+------+
| s202 |   467527 |   467527 |   0  |
| s100 |   951810 |   951810 |   0  |
| s068 |   894306 |   894306 |   0  |
| s163 |   961166 |   961166 |   0  |
+------+----------+----------+------+
*/

with step7g as (
    select
        f.value  as sid,
        count(*) as total
    from "S3_DATA_SOURCES"."CARPE_DATASTORE_COMMERCIAL"."p07/g_base",
         lateral flatten(SOURCES:source_ids) as f
    where contains(SOURCES:unique_source_ids, 's068')
    group by 1
),
     step7h as (
         select
            f.value  as sid,
            count(*) as total
         from "S3_DATA_SOURCES"."CARPE_DATASTORE_COMMERCIAL"."p07/h_base",
              lateral flatten(SOURCES:source_ids) as f
         where contains(SOURCES:unique_source_ids, 's068')
         group by 1
     )

select step7g.sid,
       step7g.total                 as "7g_total",
       step7h.total                 as "7h_total",
       step7g.total - step7h.total  as diff
from step7g
         join step7h
              on step7g.sid = step7h.sid


//ATHENA

/*
+------+----------+----------+------+
| SID  | 7g_total | 7h_total | DIFF |
+------+----------+----------+------+
| s202 |   467961 |   467961 |   0  |
| s100 |   952775 |   952775 |   0  |
| s068 |   895244 |   895244 |   0  |
| s163 |   962168 |   962168 |   0  |
+------+----------+----------+------+
*/

with step7g as (
    select
        f  as sid,
        count(*) as total
    from "p07_g",
         unnest(SOURCES.source_ids) as t(f)
    where contains(SOURCES.unique_source_ids, 's068')
    group by 1
),

    step7h as (
     select
        f  as sid,
        count(*) as total
     from "p07_h",
          unnest(SOURCES.source_ids) as t(f)
     where contains(SOURCES.unique_source_ids, 's068')
     group by 1
     )

select step7g.sid,
        step7g.total                    as "7g_total",
        step7h.total                    as "7h_total",
        step7g.total - step7h.total     as diff
from step7g
         join step7h
              on step7g.sid = step7h.sid


