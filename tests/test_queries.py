import sqlparse
import sqlparse.sql as s

from sqltrans.queries import get_function_params
from sqltrans.search import Search

"""

"""


def test_get_func_params_multiple_identifiers():
    sql = "select cast(substring(tab.field, 1, 4) as int) from tab"
    parsed_src = sqlparse.parse(sql)[0]
    func = Search(parsed_src).get(sql_class=s.Function).last().result().one()
    params = [str(i) for i in get_function_params(func)]
    assert params == ['tab.field', '1', '4']


def test_get_func_params_single_identifier():
    sql = "select max(to_date(rdt)) from tab1"
    parsed_src = sqlparse.parse(sql)[0]
    func = Search(parsed_src).get(sql_class=s.Function).last().result().one()
    params = [str(i) for i in get_function_params(func)]
    assert params == ['rdt']


def test_get_func_params_functions_and_identifiers():
    sql = "select some_func(func(1), cast(substring(tab.field, 1, 4) as int), 1, 'str') from tab1"
    parsed_src = sqlparse.parse(sql)[0]
    func = Search(parsed_src).get(sql_class=s.Function).first().result().one()
    params = [str(i) for i in get_function_params(func)]
    assert params == ['func(1)', 'cast(substring(tab.field, 1, 4) as int)', '1', "'str'"]


def test_get_func_params_single_functions():
    sql = "select some_func(substring(tab.field, 1, 4)) from tab1"
    parsed_src = sqlparse.parse(sql)[0]
    func = Search(parsed_src).get(sql_class=s.Function).first().result().one()
    params = [str(i) for i in get_function_params(func)]
    assert params == ['substring(tab.field, 1, 4)']


def test_get_func_params_no_params():
    sql = "select some_func() from tab1"
    parsed_src = sqlparse.parse(sql)[0]
    func = Search(parsed_src).get(sql_class=s.Function).first().result().one()
    params = [str(i) for i in get_function_params(func)]
    assert params == []
