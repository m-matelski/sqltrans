import sqlparse
import sqlparse.sql as s

from sqltrans.queries import get_function_params
from sqltrans.search import Search


def test_get_func_params():
    sql = "select cast(substring(tab.field, 1, 4) as int) from tab"
    parsed_src = sqlparse.parse(sql)[0]
    func = Search(parsed_src).get(sql_class=s.Function).last().result().one()
    params = [str(i) for i in get_function_params(func)]
    assert params == ['tab.field', '1', '4']


def test_get_func_params_2():
    sql = "select max(to_date(rdt)) from tab1"
    parsed_src = sqlparse.parse(sql)[0]
    func = Search(parsed_src).get(sql_class=s.Function).last().result().one()
    params = [str(i) for i in get_function_params(func)]
    assert params == ['rdt']
