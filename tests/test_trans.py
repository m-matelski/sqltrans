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
    assert translated == tgt_sql_redshift
