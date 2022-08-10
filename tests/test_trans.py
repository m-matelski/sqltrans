from copy import deepcopy

import sqlparse
from sqlparse import sql as s

from sqltrans.queries import get_function_params
from sqltrans.search import Search
from sqltrans.translate import find_route, translate
from sqltrans.utils import read_file


def test_find_route1():
    assert find_route({0: [1], 1: [2], 2: [3]}, 0, 1) == [(0, 1)]
    assert find_route({0: [1], 1: [2], 2: [3]}, 0, 2) == [(0, 1), (1, 2)]
    assert find_route({0: [1], 1: [2, 3], 2: [3], 3: [4, 5], 4: [5]}, 0, 5) == [(0, 1), (1, 3), (3, 5)]
    assert find_route({0: [1, 2], 1: [2], 2: [3], 3: [0, 1, 2]}, 0, 3) == [(0, 2), (2, 3)]
    assert find_route({0: [1], 1: [2], 2: [3]}, 0, 4) is None
    assert find_route({0: [1], 2: [3]}, 0, 3) is None


def test_spark_to_redshift_cast():
    sql = "select cast(substring(tab.field, 1, 4) as int) from tab"
    translated = translate(sql, 'spark', 'redshift')
    expected = "select int(substring(tab.field, 1, 4)) from tab"
    assert expected == translated


def test_complex_query():
    src_sql_spark = read_file('tests/spark_to_redshift/1/src.sql')
    tgt_sql_redshift = read_file('tests/spark_to_redshift/1/tgt.sql')
    translated = translate(src_sql_spark, 'spark', 'redshift', as_parsed=False)
    expected = ("WITH CTE_T1 as\n"
                "(\n"
                "    case when current_date <= fc.ad then dateadd(day, -1, dd.fpsd) else current_date end as tech_date\n"
                "    from DD dd\n"
                "),\n"
                "CTE_T2 as\n"
                "(\n"
                "    select max(fc.f1) as d\n"
                "    from FC fc\n"
                "    where current_date > fc.ad\n"
                "),\n"
                "CTE_T3 as\n"
                "(\n"
                "    select max(to_date(rdt)) from tab1\n"
                "),\n"
                "select\n"
                "current_timestamp as current_date_time,\n"
                "t.f1\n"
                "FROM MAIN_TABLE t1\n"
                "WHERE t1.status = 'DONE'\n"
                "and int(substring(t1.apy, 1, 4)) >= int(substring(t1.apx, 1, 4)) -1")
    assert translated == tgt_sql_redshift
