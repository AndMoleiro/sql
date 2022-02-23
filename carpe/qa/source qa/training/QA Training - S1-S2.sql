/////////QA TRAINING

--S163 - Healthgrades (Parent: s068)

//////STEP 1 -> STEP 2

/*

Step 1 data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"

Step 2 data: S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"

FIELDS:

V    about                                                       <- about
V    board_certifications[#]                                     <- board_certifications[#]
V    city                                                        <-  city
V    company_name                                                <-  company_name
V    education[#].date                                           <-  education[#].date
V    education[#].name                                           <-  education[#].name
V    education[#].root_type                                      <-  education[#].type
V    experience_checks_title[#]                                  <-  experience_checks[#].title
V    extract_date                                                <-  extract_date
  X  location                                                    <-  NULL
V    phone_numbers[#]                                            <-  phone_numbers[#]
V    position                                                    <-  position
V    rating                                                      <-  rating
V    rating_count                                                <-  rating_count
V    reviews[#].author                                           <-  reviews[#].author
V    reviews[#].date                                             <-  reviews[#].date
V    reviews[#].rating                                           <-  reviews[#].rating
V    reviews[#].text                                             <-  reviews[#].text
V    reviews[#].location                                         <-  reviews[#].location
V    specialties[#]                                              <-  specialties[#]
V    state                                                       <-  state
V    street                                                      <-  street
V    zip                                                         <-  zip
V    url                                                         <-  url
V    age                                                         <-  age
V    awards[#]                                                   <-  awards[#]
    care_philosophy                                             <-  care_philosophy
    gender                                                      <-  gender
    languages[#]                                                <-  languages[#]
    NEW accepts_new_patients                                    <-  accepts_new_patients
    NEW affiliated_hospitals[#].main_site_url                   <-  affiliated_hospitals[#].main_site_url
    NEW affiliated_hospitals[#].name                            <-  affiliated_hospitals[#].name
    NEW office_locations[#].office_locations[#].city            <-  office_locations[#].office_locations[#].city
    NEW office_locations[#].office_locations[#].latitude        <-  office_locations[#].office_locations[#].latitude
    NEW office_locations[#].office_locations[#].longitude       <-  office_locations[#].office_locations[#].longitude
    NEW office_locations[#].office_locations[#].main_site_url   <-  office_locations[#].office_locations[#].main_site_url
    NEW office_locations[#].office_locations[#].name            <-  office_locations[#].office_locations[#].name
    NEW office_locations[#].office_locations[#].nation          <-  office_locations[#].office_locations[#].nation
    NEW office_locations[#].office_locations[#].postal_code     <-  office_locations[#].office_locations[#].postal_code
    NEW office_locations[#].office_locations[#].state           <-  office_locations[#].office_locations[#].state
    NEW office_locations[#].office_locations[#].street          <-  office_locations[#].office_locations[#].street
    NEW office_locations[#].practice_name                       <-  office_locations[#].practice_name
    NEW practice_areas[#].clinical_focus_name                   <-  practice_areas[#].clinical_focus_name
    NEW practice_areas[#].dcp_items[#].name                     <-  practice_areas[#].dcp_items[#].name
    NEW practice_areas[#].dcp_items[#].percentile               <-  practice_areas[#].dcp_items[#].percentile
    NEW telehealth_text                                         <-  telehealth_text



*/

////OVERALL COUNTS OF SOURCES

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0

----------------------------------------------------------------------------------------------------

////HIT RATE

//about

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where about is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where about is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868864
--  ACTUAL:     868793
--  DIFF:       71


-- Entity to Entity

with input_tab as (
    select
        RESHAPED_ID,
        about as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
),
output_tab as (
    select
        RESHAPED_ID,
        about::variant as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
)

select
input_tab.*,
output_tab.*
from input_tab
left join output_tab
on input_tab.RESHAPED_ID = output_tab.RESHAPED_ID
where input_tab.dp is not null
      and output_tab.dp is null

-- The loss of 71 records comes from the normalization of strings like '.', '...', '*', etc...

--------------------------------------------------

//board_certifications[#]

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    ,lateral flatten(board_certifications) as f
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    ,lateral flatten(board_certifications) as f
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   266560
--  ACTUAL:     266560
--  DIFF:       0

--------------------------------------------------

//city

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where city is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where city is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0

--------------------------------------------------

//company_name

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where company_name is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where company_name is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0

--------------------------------------------------

//education[#].date

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(education) as e
    where e.value:date is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(education) as e
    where e.value:date is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   594302
--  ACTUAL:     594302
--  DIFF:       0

--------------------------------------------------

//education[#].date

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(education) as e
    where e.value:name is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(education) as e
    where e.value:name is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   768337
--  ACTUAL:     768319
--  DIFF:       18


-- Entity to Entity

with input_tab as (
    select
        reshaped_id,
        tools.public.array_sort(array_agg(e.value:name)) as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(education) as e
    group by 1
),
output_tab as (
    select
        reshaped_id,
        tools.public.array_sort(array_agg(e.value:name)) as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(education) as e
    group by 1
)

select
input_tab.*,
output_tab.*,
sum(array_size(input_tab.dp) - array_size(output_tab.dp))
    over (order by input_tab.reshaped_id rows between unbounded preceding and current row) as array_size_diff
from input_tab
left join output_tab
on input_tab.reshaped_id = output_tab.reshaped_id
where array_size(input_tab.dp) <> array_size(output_tab.dp)


--> The loss of 18 records comes from the normalization of strings like '.' or '..'


--------------------------------------------------

//education[#].date

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(education) as e
    where e.value:type is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(education) as e
    where e.value:root_type is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   738840
--  ACTUAL:     738840
--  DIFF:       0

--------------------------------------------------

//experience_checks_title[#]

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(experience_checks) as e
    where e.value:title is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(experience_checks_title) as e
    where e.value is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   618367
--  ACTUAL:     618367
--  DIFF:       0


//extract_date

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where extract_date is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where extract_date is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0


//location

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where location is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where location is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   617654
--  ACTUAL:     617647
--  DIFF:       7


--  Entity to Entity

with input_tab as (
    select
        reshaped_id,
        location as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
),
output_tab as (
    select
        reshaped_id,
        location as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
)

select
input_tab.*,
output_tab.*
from input_tab
left join output_tab
on input_tab.reshaped_id = output_tab.reshaped_id
where input_tab.dp is not null
      and output_tab.dp is null

--> The loss of 7 records comes from the normalization of strings like '.' or '..'

--> This should not be mapped, per the ticket's instructions.


//phone_numbers[#]

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(phone_numbers) as p
    where p.value is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(phone_numbers) as p
    where p.value is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   886356
--  ACTUAL:     886356
--  DIFF:       0


//position

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where position is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where position is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       7


//rating

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where rating is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where rating is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   503575
--  ACTUAL:     503575
--  DIFF:       0


//rating_count

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where rating_count is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where rating_count is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0


//reviews[#].author

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(reviews) as r
    where r.value:author is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(reviews) as r
    where r.value:author is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   1351901
--  ACTUAL:     1351798
--  DIFF:       103


-- Entity to Entity


with input_tab as (
    select
        reshaped_id,
        tools.public.array_sort(array_agg(r.value:author)) as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(reviews) as r
    group by 1
),
output_tab as (
    select
        reshaped_id,
        tools.public.array_sort(array_agg(r.value:author)) as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(reviews) as r
    group by 1
)

select
input_tab.*,
output_tab.*,
sum(array_size(input_tab.dp) - array_size(output_tab.dp))
    over (order by input_tab.reshaped_id rows between unbounded preceding and current row) as array_size_diff
from input_tab
left join output_tab
on input_tab.reshaped_id = output_tab.reshaped_id
where array_size(input_tab.dp) <> array_size(output_tab.dp)

--> The loss of 103 records comes from the normalization of strings like '.', '..', '****'.


//reviews[#].date

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(reviews) as r
    where r.value:date is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(reviews) as r
    where r.value:date is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   1351901
--  ACTUAL:     1351901
--  DIFF:       0


//reviews[#].rating

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(reviews) as r
    where r.value:rating is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(reviews) as r
    where r.value:rating is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   1351901
--  ACTUAL:     1351901
--  DIFF:       0


//reviews[#].text

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(reviews) as r
    where r.value:text is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(reviews) as r
    where r.value:text is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   1351864
--  ACTUAL:     1351755
--  DIFF:       109


-- Entity to Entity


with input_tab as (
    select
        reshaped_id,
        tools.public.array_sort(array_agg(r.value:text)) as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(reviews) as r
    group by 1
),
output_tab as (
    select
        reshaped_id,
        tools.public.array_sort(array_agg(r.value:text)) as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(reviews) as r
    group by 1
)

select
input_tab.*,
output_tab.*,
sum(array_size(input_tab.dp) - array_size(output_tab.dp))
    over (order by input_tab.reshaped_id rows between unbounded preceding and current row) as array_size_diff
from input_tab
left join output_tab
on input_tab.reshaped_id = output_tab.reshaped_id
where array_size(input_tab.dp) <> array_size(output_tab.dp)

--> The loss of 109 records comes from the normalization of strings like '.', '..', '****'.


//reviews[#].location

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(reviews) as r
    where r.value:location is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(reviews) as r
    where r.value:location is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   531626
--  ACTUAL:     531620
--  DIFF:       6


-- Entity to Entity


with input_tab as (
    select
        reshaped_id,
        tools.public.array_sort(array_agg(r.value:location)) as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(reviews) as r
    group by 1
),
output_tab as (
    select
        reshaped_id,
        tools.public.array_sort(array_agg(r.value:location)) as dp
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(reviews) as r
    group by 1
)

select
input_tab.*,
output_tab.*,
sum(array_size(input_tab.dp) - array_size(output_tab.dp))
    over (order by input_tab.reshaped_id rows between unbounded preceding and current row) as array_size_diff
from input_tab
left join output_tab
on input_tab.reshaped_id = output_tab.reshaped_id
where array_size(input_tab.dp) <> array_size(output_tab.dp)

--> The loss of 6 records comes from the normalization of strings like '-', '????????', ...


//specialties[#]

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(specialties) as s
    where s.value is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(specialties) as s
    where s.value is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   1144018
--  ACTUAL:     1144018
--  DIFF:       0


//state

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where state is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where state is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0


//street

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where street is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where street is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0


//zip

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where zip is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where zip is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0


//url

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where url is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where url is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   868895
--  ACTUAL:     868895
--  DIFF:       0


//age

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    where age is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    where age is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   420466
--  ACTUAL:     420466
--  DIFF:       0


//awards[#]

with input_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p01/s163"
    , lateral flatten(awards) as a
    where a.value is not null
    group by 1
),
output_tab as (
    select
        ' ' as joining,
        count(*) as t
    from S3_DATA_SOURCES.CARPE_DATASTORE_COMMERCIAL."p02/s163"
    , lateral flatten(awards) as a
    where a.value is not null
    group by 1
)

select
input_tab.t as expected,
output_tab.t as actual,
expected - actual as diff
from input_tab
left join output_tab
on input_tab.joining = output_tab.joining

--  Expected:   228165
--  ACTUAL:     228165
--  DIFF:       0