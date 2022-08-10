from sqlparse import sql as s
from sqlparse.sql import TypeParsed

from sqltrans.helpers import build_tokens, replace_token
from sqltrans.queries import get_function_name, get_function_params
from sqltrans.search import match_string, Search, SearchToken, CommonPatterns
from sqltrans.transform import TransformationCommand, Transformation, RecursiveTransformationRunner
from sqltrans.translate import Translation, build_translation
from sqltrans.translations.generic_rules import remove_parenthesis_for_function
from sqltrans.translations.utils import register_rule

local_rules: list[TransformationCommand] = []


@register_rule(local_rules)
def type_cast(parsed: TypeParsed, translation: Translation) -> None:
    if isinstance(parsed, s.Function) and match_string(get_function_name(parsed), 'cast'):
        as_token = Search(parsed) \
            .get(sql_class=s.Parenthesis).first() \
            .get(pattern='as', case_sensitive=False).last().result().one()

        casted_token = SearchToken(as_token) \
            .get_preceding() \
            .exclude(pattern=CommonPatterns.whitespaces) \
            .first().result().one()

        cast_type_token = SearchToken(as_token) \
            .get_succeeding() \
            .exclude(pattern=CommonPatterns.whitespaces) \
            .first().result().one()

        new_token = build_tokens(tokens=[str(cast_type_token), '(', casted_token, ')'],
                                 lexer=translation.tgt_parser.get_lexer())
        replace_token(parsed, new_token)


@register_rule(local_rules)
def date_add(parsed: TypeParsed, translation: Translation) -> None:
    if isinstance(parsed, s.Function) and match_string(get_function_name(parsed), 'date_add'):
        params = get_function_params(parsed)
        new_token = build_tokens(tokens=['dateadd(day, ', params[1], ', ', params[0], ')'],
                                 lexer=translation.tgt_parser.get_lexer())
        replace_token(parsed, new_token)


local_rules.append(remove_parenthesis_for_function([
    'current_date',
    'current_timestamp'
]))


@register_rule(local_rules)
def time_stamp_to_date_to_trunc(parsed: TypeParsed, translation: Translation) -> None:
    if isinstance(parsed, s.Function) and match_string(get_function_name(parsed), 'to_date'):
        params = get_function_params(parsed)
        if len(params) != 1:
            return
        new_token = build_tokens(tokens=['trunc(', params[0], ')'], lexer=translation.tgt_parser.get_lexer())
        replace_token(parsed, new_token)


trans = build_translation(src_dialect='spark', tgt_dialect='redshift', local_rules=local_rules)
