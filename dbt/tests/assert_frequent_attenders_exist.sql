/*
    C2 – Silent Failure Detection (singular test)

    Verifies that the frequent_attenders mart actually contains rows where
    is_frequent_attender = true.  If a schema change or join bug silently
    eliminated all frequent attenders, this test will fail.

    dbt convention: a singular test fails when it returns rows.
    We return a row when the count of frequent attenders is zero.
*/

select 1 as failure_flag
from (
    select count(*) as cnt
    from {{ ref('frequent_attenders') }}
    where is_frequent_attender = true
) as t
where t.cnt = 0
