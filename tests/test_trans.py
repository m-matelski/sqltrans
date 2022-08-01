from copy import deepcopy

import sqlparse
from sqlparse import sql as s

from sqltrans.helpers import get_function_params
from sqltrans.search import Search
from sqltrans.translate import find_route, translate
from sqltrans.utils import read_file


def test_dummy_parse():
    sql = "SELECT * from tab1 where 1 > 2"
    parsed = sqlparse.parse(sql)[0]
    x = 1


def test_dummy_parse2():
    sql = "SELECT t.a, t.b, case when t.d > 1 then 'bigger' else 'lower' END from tab1 where (1>2) and t.c is not null"
    parsed = sqlparse.parse(sql)[0]
    x = 1


def test_find_route1():
    assert find_route({0: [1], 1: [2], 2: [3]}, 0, 1) == [(0, 1)]
    assert find_route({0: [1], 1: [2], 2: [3]}, 0, 2) == [(0, 1), (1, 2)]
    assert find_route({0: [1], 1: [2, 3], 2: [3], 3: [4, 5], 4: [5]}, 0, 5) == [(0, 1), (1, 3), (3, 5)]
    assert find_route({0: [1, 2], 1: [2], 2: [3], 3: [0, 1, 2]}, 0, 3) == [(0, 2), (2, 3)]
    assert find_route({0: [1], 1: [2], 2: [3]}, 0, 4) is None
    assert find_route({0: [1], 2: [3]}, 0, 3) is None


def test_copy():
    sql = "SELECT * FROM tab WHERE 1 < 2"
    parsed = sqlparse.parse(sql)[0]
    c = deepcopy(parsed)
    x = 1


def test_spark_to_redshift_cast():
    sql = "select cast(substring(tab.field, 1, 4) as int) from tab"
    parsed_src = sqlparse.parse(sql)[0]
    parsed_tgt = translate(sql, 'spark', 'redshift', as_parsed=True)
    print(parsed_tgt)
    x = 1


def test_get_func_params():
    sql = "select cast(substring(tab.field, 1, 4) as int) from tab"
    parsed_src = sqlparse.parse(sql)[0]
    func = Search(parsed_src).get(sql_class=s.Function).last().result().one()
    # func = Search(parsed_src).get(sql_class=s.Function).last().result().as_list()
    params = get_function_params(func)
    x = 1


def test_it1():
    lst = list(range(10))
    for i in lst:
        print(i)
        if i==2:
            lst[2:3] = [111, 222]
        if i == 4:
            lst.remove(4)
    print(lst)


def test_complex_query():
    src_sql_spark = read_file('spark_to_redshift/1/src.sql')
    tgt_sql_redshift = read_file('spark_to_redshift/1/tgt.sql')
    parsed_src = sqlparse.parse(src_sql_spark)[0]
    translated = translate(src_sql_spark, 'spark', 'redshift', as_parsed=True)
    x = 1

