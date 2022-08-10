WITH CTE_T1 as
(
    case when current_date <= fc.ad then dateadd(day, -1, dd.fpsd) else current_date end as tech_date
    from DD dd
),
CTE_T2 as
(
    select max(fc.f1) as d
    from FC fc
    where current_date > fc.ad
),
CTE_T3 as
(
    select max(trunc(rdt)) from tab1
),
select
current_timestamp as current_date_time,
t.f1
FROM MAIN_TABLE t1
WHERE t1.status = 'DONE'
and int(substring(t1.apy, 1, 4)) >= int(substring(t1.apx, 1, 4)) -1