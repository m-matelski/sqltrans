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

    def __call__(self, parsed: TypeParsed, transform: TransformationBase) -> TypeParsed | None:
        """
        This method will be called for every element in parsed sql statement.

        Args:
            parsed: parsed sql statement to transform.
            transform: transform instance reference.

        Returns:
            new parsed object to be used to replace input parsed in parsed tree by TransformationRunner instance,
            or nothing (None) if custom replacement has been performed in call.
        """
        ...


class TransformationRunnerBase(ABC):
    def __init__(self,
                 transformation_rules: list[TransformationCommand],
                 transformation: TransformationBase | None = None):
        self.transformation_rules = transformation_rules
        self.transformation: TransformationBase | None = transformation
        self.validate_rules()

    def validate_rules(self):
        if any(not isinstance(i, TransformationCommand) for i in self.transformation_rules):
            raise ValueError(f'Invalid rule provided - not type of TranslationCommand.')

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

    def set_transformation(self, transformation: TransformationBase):
        self.transformation = transformation


class RecursiveTransformationRunner(TransformationRunnerBase):
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
            result_parsed = rule(parsed, self.transformation)

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


class StatementTransformationRunner(TransformationRunnerBase):

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
            result_parsed = rule(stmt_copy, self.transformation)
        return stmt_copy


class TransformationBase(ABC):
    """
    Entry interface for performing transformation.
    Subclasses are responsible for creating/injecting TransformationRunner and trigger it on transform call.
    TransformationBase instance will be passed to every transformation rule triggered by Runner,
    it may consist additional data necessary to perform transformation.
    """

    @abstractmethod
    def transform(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        """
        Transform parsed sql statement.
        Args:
            stmt: input statement

        Returns:
            Transformed statement.
        """
        pass


class Transformation(TransformationBase):
    def __init__(self,
                 transformation_runner: TransformationRunnerBase,
                 src_parser: SqlParser | None = None,
                 tgt_parser: SqlParser | None = None):
        self.src_parser = src_parser or GenericSqlParser()
        self.tgt_parser = tgt_parser or GenericSqlParser()
        self.transformation_runner = transformation_runner
        self.transformation_runner.transformation = self

    def transform(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        return self.transformation_runner.run(stmt)


class CompositeTransformation(TransformationBase):
    """
    Runs multiple Transformation one after the other.
    """

    def __init__(self, transforms: list[TransformationBase]):
        self.transforms = transforms

    def transform(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        return chain_func(stmt, (trans.transform for trans in self.transforms))
