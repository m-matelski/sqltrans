"""
This module consist of predefined parsed sql statement queries.
"""
from sqltrans.search import Search
import sqlparse.sql as s
import sqlparse.tokens as t


def get_function_name(parsed: s.TypeParsed) -> str:
    """
    Returns function name from input parsed token
    Args:
        parsed: input parsed token.

    Returns:
        function name
    """
    name = Search(parsed).get(sql_class=s.Identifier, levels=1).first().result().one().value
    return name


def get_function_params(parsed: s.TypeParsed) -> list[s.TypeParsed]:
    """
    Returns function parameters.
    Args:
        parsed: input parsed token.

    Returns:
        List of Tokens, which are parameter for function provided in parsed.
    """
    if isinstance(parsed, s.Function):
        return list(parsed.get_parameters())
