"""
This model defines base classes for performing transformation operations on parsed sql statements.
"""

from __future__ import annotations

from abc import ABC, abstractmethod, ABCMeta
from copy import deepcopy
from typing import runtime_checkable, Protocol, Type

import sqlparse
from sqlparse.parsers import SqlParser, GenericSqlParser
from sqlparse.sql import TypeParsed

from sqltrans.helpers import replace_token
from sqltrans.utils import ChangingListIterator, chain_func


@runtime_checkable
class TransformationCommand(Protocol):
    """
    Interface for transformation command.
    Transformation modifies statement in place, or pass modification task to TransformationRunner instance.
    """

    def __call__(self, parsed: TypeParsed, tgt_parser: SqlParser) -> TypeParsed | None:
        """
        This method will be called for every element in parsed sql statement.

        Args:
            parsed: parsed sql statement to transform.
            tgt_parser: transform instance reference.

        Returns:
            new parsed object to be used to replace input parsed in parsed tree by TransformationRunner instance,
            or nothing (None) if custom replacement has been performed in call.
        """
        ...


class TransformationRunnerBase(ABC):

    @abstractmethod
    def run(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        """
        Implement method that runs sequence of commands over input statement.
        Make sure to create a copy of input statement.

        Args:
            stmt: input statement

        Returns:
            Transformed statement.
        """
        pass


class ConcreteTransformationRunnerBase(TransformationRunnerBase, ABC):
    def __init__(self,
                 transformation_rules: list[TransformationCommand],
                 tgt_parser: SqlParser):
        self.transformation_rules = transformation_rules
        self.tgt_parser = tgt_parser
        self.validate_rules()

    def validate_rules(self):
        # TODO: this sucks because it doesn't check for protocol method signature
        if any(not isinstance(i, TransformationCommand) for i in self.transformation_rules):
            raise ValueError(f'Invalid rule provided - not type of TranslationCommand.')


class RecursiveTransformationRunner(ConcreteTransformationRunnerBase):
    """
    Runs sequence of commands over input parsed sql statement traversed recursively.
    """

    def _recursive_run(self, parsed: TypeParsed):
        """
        Perform recursive traverse over parsed tree and runs transformation rules over every element.

        Args:
            parsed: parsed sql statement, or it's nested part.
        """
        for rule in self.transformation_rules:
            result_parsed = rule(parsed, self.tgt_parser)

            if isinstance(result_parsed, sqlparse.sql.Token):
                replace_token(parsed, result_parsed)
            elif result_parsed is None:
                pass
            else:
                raise ValueError(f'Rule {rule} is expected to return type {TypeParsed} or None, '
                                 f'{result_parsed} returned.')

        if parsed.is_group:
            for i in ChangingListIterator(parsed.tokens):
                self._recursive_run(i)

    def run(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        """
        Runs sequence of commands over input statement traversed recursively.

        Args:
            stmt: input statement

        Returns:
            Transformed statement.
        """
        stmt_copy = deepcopy(stmt)
        self._recursive_run(stmt_copy)
        return stmt_copy


class StatementTransformationRunner(ConcreteTransformationRunnerBase):

    def run(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        """
        Runs sequence of commands over whole root statement.
        Args:
            stmt: input statement

        Returns:
            Transformed statement.
        """
        stmt_copy = deepcopy(stmt)
        for rule in self.transformation_rules:
            result_parsed = rule(stmt_copy, self.tgt_parser)
        return stmt_copy


class CompositeTransformationRunner(TransformationRunnerBase):
    def __init__(self, transformations: list[TransformationRunnerBase]):
        self.transformations = transformations

    def run(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        return chain_func(stmt, (trans.run for trans in self.transformations))
