"""
This module provides tools for analyzing and searching for structures in parsed sql statement token tree.
"""

from __future__ import annotations

import re
import sys
import collections.abc as collections_abc
from abc import ABC, abstractmethod
from functools import partial
from typing import Union, Type, Tuple, Generator, TypeVar, Collection, Iterable, List, Sequence, Literal

import sqlparse.sql as s
from sqlparse.tokens import TokenType
from sqlparse.sql import TypeParsed

from sqltrans.utils import listify

# region Typing
T = TypeVar('T')
C = TypeVar('C')
PatternType = Union[str, re.Pattern]

OneOrIterable = Iterable[T] | T
OneOrList = List[T] | T
OneOrTuple = Tuple[T, ...] | T
OneOrCollection = Collection[T] | T


# endregion


# region Parsed Search helper functions
def match_sql_class(parsed: TypeParsed, sql_class: OneOrTuple[Type[s.Token]]) -> bool:
    """
    Test if parsed is one of provided sql classes to match.
    :param parsed: parsed
    :param sql_class: sql class - Token and subclasses in hierarchy.
    :return: True if match found.
    """
    return isinstance(parsed, sql_class)


def match_token_type(parsed: TypeParsed, ttype: OneOrTuple[Type[TokenType]]) -> bool:
    """
    Test parsed token type against Token Types.
    :param parsed: parsed
    :param ttype: token type - provide lower token type in  hierarchy for more strict checks.
    :return: True if match found.
    """
    return parsed.ttype in ttype


def match_string(string: str, pattern: OneOrIterable[PatternType], case_sensitive=False) -> bool:
    """
    Test string against provided patterns. If string matches any pattern returns true. False otherwise.

    :param string: string to test
    :param pattern: pattern or patterns to test string against. Can be a string, re, or regex string.
    :param case_sensitive: Whether string to pattern match have to be case-sensitive
    :return: True if string matches any of provided patterns.
    """
    flags = 0
    flags = re.IGNORECASE if not case_sensitive else flags

    pattern = listify(pattern)

    for p in pattern:
        if isinstance(p, str):
            match = bool(re.fullmatch(p, string, flags))
        elif isinstance(p, re.Pattern):
            match = bool(p.fullmatch(string))
        else:
            raise ValueError(f'Invalid object type in pattern: {p}')
        if match:
            return True
    return False


def match_token_value(parsed: TypeParsed, pattern: OneOrIterable[PatternType], case_sensitive=False) -> bool:
    """
    Match token's value with provided pattern.

    :param parsed: input token
    :param pattern: pattern or patterns to test string against. Can be a string, re, or regex string.
    :param case_sensitive: Whether string to pattern match have to be case-sensitive
    :return: True if token's value matches any of provided patterns.
    """
    return match_string(parsed.value, pattern, case_sensitive)


def match_parsed(parsed: TypeParsed,
                 sql_class: OneOrTuple[Type[s.Token]] | None = None,
                 ttype: OneOrTuple[Type[TokenType]] | None = None,
                 pattern: OneOrIterable[PatternType] | None = None,
                 case_sensitive=False) -> bool:
    """
    Matching parsed against: sql class, token type, and token value. Returns True if any match found.
    Sql class and token type are mutually exclusive - there is no sense providing them both.
    If Sql class or token type is provided without pattern - token value check is not performed.
    If Sql class or token type is provided without pattern
        - value check against pattern is performed when token matches type or class.
    If only pattern is provided - value check against pattern is performed whatever token type or class it is.

    :param parsed: parsed statement (or it's part)
    :param sql_class: sql class - Token and subclasses in hierarchy.
    :param ttype: token type - provide lower token type in hierarchy for more strict checks.
    :param pattern: pattern or patterns to test string against. Can be a string, re, or regex string.
    :param case_sensitive: Whether string to pattern match have to be case-sensitive
    :return: True if token matches description.
    """
    class_type_match = (sql_class and match_sql_class(parsed, sql_class)) or \
                       (ttype and match_token_type(parsed, ttype)) or (sql_class is None and ttype is None)
    pattern_match = (pattern is None or match_token_value(parsed, pattern, case_sensitive))
    return class_type_match and pattern_match


def identity(a):
    return a


def neg(a) -> bool:
    return not a


def search_parsed(parsed: TypeParsed | Iterable[s.Token],
                  sql_class: OneOrTuple[Type[s.Token]] | None = None,
                  ttype: OneOrTuple[Type[TokenType]] | None = None,
                  pattern: OneOrIterable[PatternType] | None = None,
                  case_sensitive=False, levels=sys.maxsize, exclude=False) -> Generator[TypeParsed, None, None]:
    """
    Performs recursive search on provided parsed statement (or list of parsed statements),
    yields token if meets input condition.

    :param parsed: parsed statement (or it's part), or any Iterable of parsed statements
    :param sql_class: sql class - Token and subclasses in hierarchy.
    :param ttype: token type - provide lower token type in hierarchy for more strict checks.
    :param pattern: pattern or patterns to test string against. Can be a string, re, or regex string.
    :param case_sensitive: Whether string to pattern match have to be case-sensitive
    :param levels: how deep recursive search should be. 1 is for no recursion.
    :param exclude: Whether to yield or not if token match condition.
    :return: Generator of tokens matching input conditions.
    """
    bool_func = neg if exclude else identity
    if isinstance(parsed, collections_abc.Iterable) and levels > 0:
        parsed = list(parsed)
        for i in parsed:
            if bool_func(match_parsed(parsed=i, sql_class=sql_class, ttype=ttype,
                                      pattern=pattern, case_sensitive=case_sensitive)):
                yield i
            yield from search_parsed(i, sql_class, ttype, pattern, case_sensitive, levels - 1, exclude)


class ParsedSearcher(ABC):
    """
    Class wrapper for search parsed function.
    Allows to store search criteria, perform lazy search, pass Searcher configuration as argument.
    """

    def __init__(self,
                 sql_class: OneOrTuple[Type[s.Token]] | None = None,
                 ttype: OneOrTuple[Type[TokenType]] | None = None,
                 pattern: OneOrIterable[PatternType] | None = None,
                 case_sensitive=False,
                 levels=sys.maxsize):
        self.sql_class = sql_class
        self.ttype = ttype
        self.pattern = pattern
        self.case_sensitive = case_sensitive
        self.levels = levels

    def __call__(self, parsed: TypeParsed | Iterable[s.Token]) -> Generator[TypeParsed, None, None]:
        return self.search(parsed)

    @abstractmethod
    def search(self, parsed: TypeParsed | Iterable[s.Token]) -> Generator[TypeParsed, None, None]:
        """
        Implement this method to define search criteria.

        Args:
            parsed: input parsed statement

        Returns: Generator of Tokens - probably the result of search_parsed function.

        """
        pass


class Match(ParsedSearcher):

    def search(self, parsed: TypeParsed | Iterable[s.Token]) -> Generator[TypeParsed, None, None]:
        """Returns tokens matching criteria."""
        return search_parsed(
            parsed=parsed,
            sql_class=self.sql_class,
            ttype=self.ttype,
            pattern=self.pattern,
            case_sensitive=self.case_sensitive,
            levels=self.levels,
            exclude=False
        )


class Exclude(ParsedSearcher):

    def search(self, parsed: TypeParsed | Iterable[s.Token]) -> Generator[TypeParsed, None, None]:
        """Returns all tokens except those matching any criteria."""
        return search_parsed(
            parsed=parsed,
            sql_class=self.sql_class,
            ttype=self.ttype,
            pattern=self.pattern,
            case_sensitive=self.case_sensitive,
            levels=self.levels,
            exclude=True
        )


class MatchAll(ParsedSearcher):
    """Returns all tokens."""

    def search(self, parsed: TypeParsed | Iterable[s.Token]) -> Generator[TypeParsed, None, None]:
        return search_parsed(parsed=parsed, exclude=True)


# endregion

# single token tools
def get_token_idx(token: s.Token) -> int | None:
    """
    Returns index of token in parent tokens list.
    """
    try:
        return token.parent.tokens.index(token)
    except ValueError:
        return None


def get_token_neighbours(token: s.Token, left: int | None, right: int | None, include_self: bool = False
                         ) -> List[s.Token]:
    """
    Returns token neighbours from token's parent token list.

    :param token: Input token
    :param left: how many neighbour tokens from token to the beginning should be returned. All if None Provided.
    :param right: how many neighbour tokens from token to the end should be returned. All if None Provided.
    :param include_self: whether to include input token in a list (keeping the order)
    :return: list of neighbour tokens
    """
    left = sys.maxsize if left is None else left
    right = sys.maxsize if right is None else right
    token_idx = get_token_idx(token)
    plist = token.parent.tokens
    tokens = plist[token_idx - left: token_idx] + ([token] if include_self else []) + plist[token_idx + 1: right]
    return tokens


def get_preceding_tokens(token: s.Token, how_many: int | None = None, include_self=False) -> List[s.Token]:
    """
    Returns preceding tokens in current token group.
    :param token: Input token
    :param how_many: How many preceding tokens should be returned (all if None provided)
    :param include_self: whether to include input token in result list.
    :return: List of preceding tokens.
    """
    return get_token_neighbours(token=token, left=how_many, right=0, include_self=include_self)


def get_succeeding_tokens(token: s.Token, how_many: int | None = None, include_self=False) -> List[s.Token]:
    """
    Returns succeeding tokens in current token group.
    :param token: Input token
    :param how_many: How many succeeding tokens should be returned (all if None provided)
    :param include_self: whether to include input token in result list
    :return: List of succeeding tokens.
    """
    return get_token_neighbours(token=token, left=0, right=how_many, include_self=include_self)


# Search fluent Interface
class ParsedQueryable(ABC):
    @abstractmethod
    def get(self,
            sql_class: OneOrTuple[Type[s.Token]] | None = None,
            ttype: OneOrTuple[Type[TokenType]] | None = None,
            pattern: OneOrIterable[PatternType] | None = None,
            case_sensitive: bool = False,
            levels: int = sys.maxsize
            ) -> SearchStep:
        """
        Returns all Tokens matching condition from current query result.
        If sql_class and ttype are both provided, then check for either sql_class match or ttype match is done.
        If pattern is provided with sql_class and/or ttype then method checks for (sql_class or ttpye) and pattern.

        Args:
            sql_class: sql class or tuple of sql classes to check, if Tuple provided, check for any match.
            ttype: token type or tuple of token types, if Tuple provided, check for any match.
            pattern: pattern or tuple of patterns to test token value against, check for any match.
            case_sensitive: Whether string pattern checks have to be case-sensitive.
            levels: How deep recursive search should be performed.

        Returns:
            result step
        """
        pass

    @abstractmethod
    def get_all(self, levels: int = sys.maxsize) -> SearchStep:
        """
        Scans recursively through all tokens, depends on maximum level.

        Args:
            levels: How deep recursive search should be performed.

        Returns:
            search step
        """
        pass


class Search(ParsedQueryable):
    """
    Search class is entry point for parsed search mechanism with fluent API.
    It takes parsed sql statement, or Iterable of parsed statements, and allows to perform query on it.
    """

    def __init__(self, parsed: OneOrIterable[TypeParsed]):
        """
        Args:
            parsed: parsed statement, or iterable of parsed statements
        """
        self.parsed = parsed

    def get(self,
            sql_class: OneOrTuple[Type[s.Token]] | None = None,
            ttype: OneOrTuple[Type[TokenType]] | None = None,
            pattern: OneOrIterable[PatternType] | None = None,
            case_sensitive=False,
            levels=sys.maxsize
            ) -> SearchStep:
        result_set = search_parsed(parsed=self.parsed, sql_class=sql_class, ttype=ttype, pattern=pattern,
                                   case_sensitive=case_sensitive, levels=levels)
        return SearchStep(result_set)

    def get_all(self, levels=sys.maxsize) -> SearchStep:
        result_set = search_parsed(parsed=self.parsed, levels=levels)
        return SearchStep(result_set)


class SearchStep(ParsedQueryable):
    """
    SearchStep is initiated after first Search.get call. Every next query method call will return SearchStep instance.
    It is entry point to get query result, or perform token-type search on current result set.
    """

    def __init__(self, parsed: OneOrIterable[TypeParsed]):
        """
        Args:
            parsed: parsed statement, or iterable of Type Parsed (usually result of search, or SearchStep)
        """
        self.parsed = parsed

    def result(self) -> SearchResult:
        """
        Returns:
            Query result.
        """
        return SearchResult(self.parsed)

    def get(self,
            sql_class: OneOrTuple[Type[s.Token]] | None = None,
            ttype: OneOrTuple[Type[TokenType]] | None = None,
            pattern: OneOrIterable[PatternType] | None = None,
            case_sensitive=False,
            levels=sys.maxsize
            ) -> SearchStep:
        # Delegate to Search instance
        return Search(self.parsed).get(sql_class=sql_class, ttype=ttype, pattern=pattern,
                                       case_sensitive=case_sensitive, levels=levels)

    def get_all(self, levels=sys.maxsize) -> SearchStep:
        # Delegate to Search instance
        return Search(self.parsed).get_all(levels=levels)

    def exclude(self,
                sql_class: OneOrTuple[Type[s.Token]] | None = None,
                ttype: OneOrTuple[Type[TokenType]] | None = None,
                pattern: OneOrIterable[PatternType] | None = None,
                case_sensitive=False,
                levels=sys.maxsize
                ) -> SearchStep:
        """
        Excludes all Tokens matching criteria from current query result.
        If sql_class and ttype are both provided, then check for either sql_class match or ttype match is done.
        If pattern is provided with sql_class and/or ttype then method checks for (sql_class or ttpye) and pattern.

        Args:
            sql_class: sql class or tuple of sql classes to check, if Tuple provided, check for any match.
            ttype: token type or tuple of token types, if Tuple provided, check for any match.
            pattern: pattern or tuple of patterns to test token value against, check for any match.
            case_sensitive: Whether string pattern checks have to be case-sensitive.
            levels: How deep recursive search should be performed.

        Returns:
            result step
        """
        result_set = search_parsed(parsed=self.parsed, sql_class=sql_class, ttype=ttype, pattern=pattern,
                                   case_sensitive=case_sensitive, levels=levels, exclude=True)
        return SearchStep(result_set)

    def top(self, n: int) -> SearchStep:
        if not n > 0:
            raise ValueError('n must be > 0')
        return SearchStep(list(self.parsed)[:n])

    def bottom(self, n: int) -> SearchStep:
        if not n > 0:
            raise ValueError('n must be > 0')
        return SearchStep(list(reversed(list(self.parsed)))[:n])

    def _get_one(self, idx: int) -> SearchStep:
        """
        Helper function for getting single element from current result.
        Args:
            idx: index of element to return

        Returns:
            search step
        """
        parsed_list = list(self.parsed)
        if not parsed_list:
            parsed = []
        else:
            parsed = parsed_list[idx]
        return SearchStep(parsed)

    def first(self) -> SearchStep:
        """
        Sets first token from current query resul (if any) as current query result.
        Note: next search step iteration will be performed on token itself, so token will be omitted.
        Returns:
            search step
        """
        return self._get_one(0)

    def last(self) -> SearchStep:
        """
        Sets last token from current query resul (if any) as current query result.
        Note: next search step iteration will be performed on token itself, so token will be omitted.
        Returns:
            search step
        """
        return self._get_one(-1)

    def preceded_by(self, sql_class: OneOrTuple[Type[s.Token]] | None = None,
                    ttype: OneOrTuple[Type[TokenType]] | None = None,
                    pattern: OneOrIterable[PatternType] | None = None,
                    case_sensitive=False, levels=sys.maxsize, search_in: ParsedSearcher | None = None) -> SearchStep:
        """
        Extracts tokens from current query result tokens that are preceded by token meeting criteria.
        It takes first token meeting search_in criteria and test it against provided criteria.
        If match not found, token is filtered out from final result.

        Args:
            sql_class: sql class match criteria for preceding token.
            ttype: token type match criteria for preceding token.
            pattern: token value pattern match criteria for preceding token.
            case_sensitive: whether pattern match should be case-sensitive.
            levels: How deep recursive search should be.
            search_in: Allows to filter out preceding tokens. Might be used for example to exclude whitespaces tokens,
                so the first non whitespace preceding tokens will be tested against criteria match.

        Returns:
            SearchStep filtered out with tokens meeting criteria.

        """
        return self._get_preceded_or_succeeded_by(
            method_name='get_preceding',
            sql_class=sql_class,
            ttype=ttype,
            pattern=pattern,
            case_sensitive=case_sensitive,
            levels=levels,
            search_in=search_in
        )

    def succeeded_by(self, sql_class: OneOrTuple[Type[s.Token]] | None = None,
                     ttype: OneOrTuple[Type[TokenType]] | None = None,
                     pattern: OneOrIterable[PatternType] | None = None,
                     case_sensitive=False, levels=sys.maxsize, search_in: ParsedSearcher | None = None) -> SearchStep:
        """
        Extracts tokens from current query result tokens that are succeeded by token meeting criteria.
        It takes first token meeting search_in criteria and test it against provided criteria.
        If match not found, token is filtered out from final result.

        Args:
            sql_class: sql class match criteria for preceding token.
            ttype: token type match criteria for preceding token.
            pattern: token value pattern match criteria for preceding token.
            case_sensitive: whether pattern match should be case-sensitive.
            levels: How deep recursive search should be.
            search_in: Allows to filter out succeeding tokens. Might be used for example to exclude whitespaces tokens,
                so the first non whitespace succeeding tokens will be tested against criteria match.

        Returns:
            SearchStep filtered out with tokens meeting criteria.

        """
        return self._get_preceded_or_succeeded_by(
            method_name='get_succeeding',
            sql_class=sql_class,
            ttype=ttype,
            pattern=pattern,
            case_sensitive=case_sensitive,
            levels=levels,
            search_in=search_in
        )

    def _get_preceded_or_succeeded_by(
            self,
            method_name: Literal['get_preceding', 'get_succeeding'],
            sql_class: OneOrTuple[Type[s.Token]] | None = None,
            ttype: OneOrTuple[Type[TokenType]] | None = None,
            pattern: OneOrIterable[PatternType] | None = None,
            case_sensitive=False, levels=sys.maxsize, search_in: ParsedSearcher | None = None) -> SearchStep:
        """
        Parametrized method to extract tokens from current query result that are preceded pr succeded
        by a token meeting criteria.
        Args:
            method_name:
            sql_class:
            ttype:
            pattern:
            case_sensitive:
            levels:
            search_in:

        Returns:

        """
        search_in = search_in or MatchAll()
        tokens = []

        methods = {
            'get_preceding': lambda x: x.get_preceding(nearest_first=True),
            'get_succeeding': lambda x: x.get_succeeding()
        }
        method = methods[method_name]

        for i in self.parsed:
            # Search will perform recursive search and flatten the result
            followed = Search(search_in(method(SearchToken(i)).result().as_list())) \
                .get(sql_class=sql_class, ttype=ttype, pattern=pattern, case_sensitive=case_sensitive, levels=levels) \
                .first().result().one_or_none()
            if followed:
                tokens.append(i)
        return SearchStep(tokens)

    def search_token(self) -> SearchToken:
        """
        Perform token type search over a single token. If current result doesn't contain a single token
        it will raise InvalidSearchable exception.
        :return: SearchToken instance to query.
        """
        try:
            result = self.result().one()
        except SearchResultException:
            raise InvalidSearchable('Cannot perform search token on a search result which is not a single token.')
        return SearchToken(result)


class SearchResultException(Exception):
    pass


class SearchTokenException(Exception):
    pass


class InvalidSearchable(Exception):
    pass


class SearchResult(collections_abc.Sequence):
    """Class for getting values from Search result."""

    def __init__(self, parsed: OneOrIterable[TypeParsed]):
        self.parsed = parsed
        self.values = list(self.__values())

    def __values(self) -> Generator[TypeParsed, None, None]:
        if isinstance(self.parsed, s.Token):
            yield self.parsed
        else:
            for i in self.parsed:
                yield i

    def as_list(self) -> List[TypeParsed]:
        """
        Return search result as list.
        Returns:
            list of tokens
        """
        return self.values

    def one(self) -> TypeParsed:
        """
        Returns only one element,
        raises SearchResultException if there is other number of tokens than 1 in search result.
        Returns:
            Result token
        """
        if len(self.values) != 1:
            raise SearchResultException(f'Expected single value, got {len(self.values)}')
        return self.values[0]

    def one_or_none(self) -> TypeParsed | None:
        """
        Returns only one element,
        or None if there is other number of tokens than 1 in search result.
        Returns:
            Result token or None
        """
        try:
            return self.one()
        except SearchResultException:
            return None

    def is_empty(self) -> bool:
        """
        Checks if search result is empty.
        Returns:
            True if search result is empty
        """
        return bool(self.values)

    def __iter__(self) -> Iterable[TypeParsed]:
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)

    def __getitem__(self, n) -> Sequence[TypeParsed]:
        return self.values[n]

    def __bool__(self):
        return not (self.is_empty())


class SearchToken:
    """
    Class for performing search on a group's token list level.
    """

    def __init__(self, token: s.Token):
        if not isinstance(token, s.Token):
            raise InvalidSearchable('Searchable must be an instance of a Token.')
        self.token = token

    def get_preceding(self, how_many: int | None = None, include_self=False, nearest_first=True) -> SearchStep:
        """
        Returns preceding tokens from token's level list.
        Args:
            how_many: How many preceding neighbour tokens to return (None to return all).
            include_self: If return token from which search is started.
            nearest_first: Whether start list from token nearest, or from left.

        Returns:
            search step
        """
        tokens = get_preceding_tokens(self.token, how_many, include_self)
        if nearest_first:
            tokens = reversed(tokens)
        return SearchStep(tokens)

    def get_succeeding(self, how_many: int | None = None, include_self=False) -> SearchStep:
        """
        Returns succeeding tokens from token's level list.
        Args:
            how_many: How many succeeding neighbour tokens to return (None to return all).
            include_self: If return token from which search is started.

        Returns:
            search step
        """
        tokens = get_succeeding_tokens(token=self.token, how_many=how_many, include_self=include_self)
        return SearchStep(tokens)

    def get_neighbours(self, left: int | None = None, right: int | None = None,
                       include_self: bool = False) -> SearchStep:
        """
        Returns preceding and succeeding tokens from token's level list.
        Args:
            left: How many preceding neighbour tokens to return (None to return all).
            right: How many succeeding neighbour tokens to return (None to return all).
            include_self: If return token from which search is started.

        Returns:
            search step
        """
        tokens = get_token_neighbours(token=self.token, left=left, right=right, include_self=include_self)
        return SearchStep(tokens)

    def get_all_neighbours(self, include_self=False) -> SearchStep:
        """
        Returns all tokens from token's level list.
        Args:
            include_self: If return token from which search is started.

        Returns:
            search step
        """
        tokens = get_token_neighbours(token=self.token, left=None, right=None, include_self=include_self)
        return SearchStep(tokens)


class CommonPatterns:
    whitespaces = re.compile(r'\s')
