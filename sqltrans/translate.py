from __future__ import annotations
from abc import ABC, abstractmethod
from collections import UserDict
from typing import List, Tuple, Any, Collection, Mapping, Optional

Optional, Mapping

import sqlparse
from sqlparse.parsers import get_parser, SqlParser
from sqlparse.sql import TypeParsed

from sqltrans.exceptions import TranslationNotFoundException
from sqltrans.search import OneOrList
from sqltrans.transform import TransformationCommand, TransformationBase, RecursiveTransformationRunner, Transformation, \
    CompositeTransformation, StatementTransformationRunner
from sqltrans.utils import chain_func


class TranslationBase(ABC):
    """
    Base class for Translation.
    """
    def __init__(self,
                 src_dialect: str,
                 tgt_dialect: str):
        self.src_dialect = src_dialect
        self.tgt_dialect = tgt_dialect

    @abstractmethod
    def translate(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        pass


class Translation(TranslationBase):
    """
    Sql statement translation between source and target dialect.
    """
    def __init__(self,
                 src_dialect: str,
                 tgt_dialect: str,
                 transformation: TransformationBase,
                 src_parser: SqlParser | None = None,
                 tgt_parser: SqlParser | None = None,
                 register=True):
        super().__init__(src_dialect, tgt_dialect)
        self.transformation = transformation
        self.src_parser = src_parser or get_parser(src_dialect)
        self.tgt_parser = tgt_parser or get_parser(tgt_dialect)
        if register:
            register_translation(self)

    def translate(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        return self.transformation.transform(stmt)


class CompositeTranslation(TranslationBase):
    """
    Composite translation allows to run multiple nested translation using the same interface.
    It might be useful for translation using intermediate sql dialect.
    """
    def __init__(self,
                 src_dialect: str,
                 tgt_dialect: str,
                 translations: List[TranslationBase]):
        super().__init__(src_dialect, tgt_dialect)
        self.translations = translations

    def translate(self, stmt: sqlparse.sql.Statement) -> sqlparse.sql.Statement:
        return chain_func(stmt, (trans.translate for trans in self.translations))


class TranslationMapping(UserDict):
    """
    Data structure for storing translation configuration for specific source and target sql dialects.
    """
    def register_translation(self, src_dialect: str, tgt_dialect: str, translation: TranslationBase, overwrite=False):
        """
        Register translation configuration.
        Args:
            src_dialect: source dialect
            tgt_dialect: target dialect
            translation: translation
            overwrite: Whether to overwrite translation configuration if it already exists in structure.
        """
        trans = self.setdefault(src_dialect, {})
        if tgt_dialect in trans and overwrite:
            raise ValueError(f"Translation from {src_dialect} to {tgt_dialect} already exists. "
                             f"Use overwrite=True if You want to overwrite a translation")
        else:
            trans[tgt_dialect] = translation

    def get_translation(self, src_dialect: str, tgt_dialect: str) -> TranslationBase:
        """
        Get translation for given source and target sql dialects.
        Args:
            src_dialect:
            tgt_dialect:

        Returns:

        """
        return self[src_dialect][tgt_dialect]


translations_meta = TranslationMapping()


def register_translation(translation: TranslationBase, overwrite=False, trans_meta=translations_meta):
    """
    Register translation configuration.
    Args:
        translation: translation object.
        overwrite: Whether to overwrite translation configuration if it already exists in structure.
        trans_meta: TranslationMapping reference to perform register operation on.
    """
    trans_meta.register_translation(translation.src_dialect, translation.tgt_dialect, translation, overwrite)


def _find_edges(pairs: Mapping[Any, Collection[Any]], src, tgt, keys=None):
    keys = keys or [src]
    if src == tgt:
        return keys
    if src in pairs:
        new_keys = [k for neighbour in pairs[src]
                    if (k := _find_edges(pairs, neighbour, tgt, keys + [neighbour])) is not None]
        best = min(new_keys, key=lambda x: len(x)) if new_keys else None
        return best
    else:
        return None


def find_route(pairs: Mapping[Any, Collection[Any]], src, tgt) -> Optional[List[Tuple[Any, Any]]]:
    points = _find_edges(pairs, src, tgt)
    result = list(zip(points, points[1:])) if points else None
    return result


def find_translation(src_dialect: str,
                     tgt_dialect: str,
                     trans_meta: TranslationMapping) -> Optional[TranslationBase]:
    route = find_route(trans_meta, src_dialect, tgt_dialect)
    if not route:
        return None
    if len(route) == 1:
        src, tgt = route[0]
        return trans_meta.get_translation(src, tgt)
    elif len(route) > 1:
        # build composite translation on a fly
        translations = [trans_meta.get_translation(src, tgt) for src, tgt in route]
        translation = CompositeTranslation(src_dialect, tgt_dialect, translations)
        return translation


def build_translation(
        src_dialect: str,
        tgt_dialect: str,
        src_parser: SqlParser | None = None,
        tgt_parser: SqlParser | None = None,
        register: bool = True,
        global_rules: list[TransformationCommand] | None = None,
        local_rules: list[TransformationCommand] | None = None) -> TranslationBase:

    global_rules = [] if global_rules is None else global_rules
    local_rules = [] if local_rules is None else local_rules

    src_parser = src_parser or get_parser(src_dialect)
    tgt_parser = tgt_parser or get_parser(tgt_dialect)

    translation = Translation(
        src_dialect=src_dialect,
        tgt_dialect=tgt_dialect,
        src_parser=src_parser,
        tgt_parser=tgt_parser,
        register=register,
        transformation=CompositeTransformation(
            transforms=[
                Transformation(
                    transformation_runner=StatementTransformationRunner(
                        transformation_rules=global_rules
                    ),
                    src_parser=src_parser,
                    tgt_parser=tgt_parser
                ),
                Transformation(
                    transformation_runner=RecursiveTransformationRunner(
                        transformation_rules=local_rules
                    ),
                    src_parser=src_parser,
                    tgt_parser=tgt_parser
                ),
            ]
        )
    )
    return translation


def translate(sql: str,
              src_dialect: str,
              tgt_dialect: str,
              encoding=None,
              src_parser: SqlParser | None = None,
              tgt_parser: SqlParser | None = None,
              trans_meta: TranslationMapping = translations_meta,
              translation: TranslationBase | None = None,
              as_parsed=False, ensure_list=False,
              ) -> OneOrList[TypeParsed | str]:
    src_parser = src_parser or get_parser(src_dialect)
    tgt_parser = tgt_parser or get_parser(tgt_dialect)
    translation = translation or find_translation(src_dialect, tgt_dialect, trans_meta)

    if not translation:
        raise TranslationNotFoundException(f"Couldn't find {src_dialect} to {tgt_dialect} translation.")

    parsed = list(src_parser.parse(sql, encoding))
    translated = [translation.translate(stmt) for stmt in parsed]
    result = translated

    if not as_parsed:
        result = [str(i) for i in result]
    if not ensure_list and len(result) == 1:
        result = result[0]
    return result
