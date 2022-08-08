import sqlparse
import sqlparse.sql as s
import sqlparse.tokens as t

from sqltrans.search import Search, CommonPatterns, Exclude


def test_search_for_keywords():
    sql = "SELECT substring(a, 1, 4), f2 FROM tab1"
    parsed = sqlparse.parse(sql)[0]

    result = list(map(str, Search(parsed).get(ttype=t.Keyword).result().as_list()))
    expected = ['SELECT', 'FROM']
    assert result == expected


def test_find_specific_function_call():
    """Testing searching for specific sql class with pattern."""
    sql = "select cast(substring(tab.field, 1, 4) as int) from tab"
    parsed = sqlparse.parse(sql)[0]

    result = Search(parsed).get(sql_class=s.Function, pattern='cast.*').result().one()
    assert str(result) == 'cast(substring(tab.field, 1, 4) as int)'


def test_extracting_function_params():
    sql = "select cast(substring(tab.field, 1, 4) as int) from tab"
    parsed = sqlparse.parse(sql)[0]

    result = Search(parsed) \
        .get(sql_class=s.Function, pattern='substring.*').first() \
        .get(sql_class=s.IdentifierList).first() \
        .exclude(ttype=(t.Punctuation, t.Whitespace), levels=1) \
        .result().as_list()
    result = [str(i) for i in result]
    assert result == ['tab.field', '1', '4']


def test_search_for_spark_cast_type():
    sql = "select cast(substring(tab.field, 1, 4) as int) from tab"
    parsed = sqlparse.parse(sql)[0]

    result = Search(parsed) \
        .get(sql_class=s.Function, levels=1) \
        .get(sql_class=s.Parenthesis).first() \
        .get(pattern='as', case_sensitive=False).last() \
        .search_token() \
        .get_succeeding() \
        .exclude(pattern=CommonPatterns.whitespaces) \
        .first().result().one()
    assert str(result) == 'int'


def test_searching_for_tables():
    sql = "SELECT * FROM (SELECT * FROM tab1 LEFT JOIN my_schema.tab2) INNER JOIN tab3 ON 1=1"
    parsed = sqlparse.parse(sql)[0]

    result = Search(parsed) \
        .get(sql_class=s.Identifier) \
        .preceded_by(ttype=t.Keyword, pattern=(r'.*JOIN.*', 'FROM'), search_in=Exclude(ttype=t.Whitespace)) \
        .result().as_list()
    result = [str(i) for i in result]
    assert result == ['tab1', 'my_schema.tab2', 'tab3']


def test_searching_for_tables_without_schema():
    sql = "SELECT * FROM (SELECT * FROM tab1 LEFT JOIN my_schema.tab2) INNER JOIN tab3 ON 1=1"
    parsed = sqlparse.parse(sql)[0]

    result = Search(parsed) \
        .get(sql_class=s.Identifier) \
        .preceded_by(ttype=t.Keyword, pattern=(r'.*JOIN.*', 'FROM'), search_in=Exclude(ttype=t.Whitespace)) \
        .exclude(pattern=r'.*\..*', levels=1) \
        .result().as_list()
    result = [str(i) for i in result]
    assert result == ['tab1', 'tab3']
