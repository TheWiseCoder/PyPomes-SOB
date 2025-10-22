from __future__ import annotations  # allow forward references
import sys
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from enum import StrEnum
from importlib import import_module
from inspect import FrameInfo, stack
from enum import Enum
from logging import Logger
from pathlib import Path
from pypomes_core import dict_clone, dict_stringify, exc_format
from pypomes_db import (
    DbEngine, db_exists, db_count,
    db_select, db_insert, db_update, db_delete
)
from types import ModuleType
from typing import Any, TypeVar

from .sob_config import (
    SOB_BASE_FOLDER, SOB_MAX_THREADS,
    sob_db_specs, sob_col_names, sob_attrs_input, sob_attrs_unique
)

# 'Sob' stands for all subclasses of 'PySob'
Sob = TypeVar("Sob",
              bound="PySob")


class PySob:
    """
    Root entity.
    """

    def __init__(self,
                 __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
                 /,
                 where_data: dict[str, Any] = None,
                 db_engine: DbEngine = None,
                 db_conn: Any = None,
                 errors: list[str] = None,
                 logger: Logger = None) -> None:

        self._logger: Logger = logger
        # maps to the entity's PK in its DB table (returned on INSERT operations)
        self.id: int | str | None = None

        # determine whether this instance exists in the database
        self._is_new: bool = True

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        if where_data:
            self.set(data=where_data)
            self.load(__references,
                      omit_nulls=True,
                      db_engine=db_engine,
                      db_conn=db_conn,
                      errors=errors)

    def insert(self,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool:

        # prepare data for INSERT
        return_col: dict[str, type] | None = None
        insert_data: dict[str, Any] = self.get()
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        is_identity: bool = sob_db_specs[cls_name][2]
        if is_identity:
            # PK is an identity column
            pk_name: str = sob_col_names[cls_name][0]
            pk_type: type = sob_db_specs[cls_name][1]
            insert_data.pop(pk_name, None)
            return_col = {pk_name: pk_type}

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # execute the INSERT statement
        tbl_name = sob_db_specs[cls_name][0]
        rec: tuple[Any] = db_insert(insert_stmt=f"INSERT INTO {tbl_name}",
                                    insert_data=insert_data,
                                    return_cols=return_col,
                                    engine=db_engine,
                                    connection=db_conn,
                                    committable=committable,
                                    errors=errors,
                                    logger=self._logger)
        if not errors:
            self._is_new = False
            if is_identity:
                # PK is an identity column
                self.id = rec[0]
        elif self._logger:
            self._logger.error(msg="Error INSERTing into table "
                                   f"{tbl_name}: {'; '.join(errors)}")
        return not errors

    def update(self,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool:

        # prepare data for UPDATE
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        update_data: dict[str, Any] = self.get(omit_nulls=False)
        tbl_name: str = sob_db_specs[cls_name][0]
        pk_name = sob_col_names[cls_name][0]
        key: int | str = update_data.pop(pk_name)

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # execute the UPDATE statement
        db_update(update_stmt=f"UPDATE {tbl_name}",
                  update_data=update_data,
                  where_data={pk_name: key},
                  min_count=1,
                  max_count=1,
                  engine=db_engine,
                  connection=db_conn,
                  committable=committable,
                  errors=errors,
                  logger=self._logger)

        if errors and self._logger:
            self._logger.error(msg="Error UPDATEing table "
                                   f"{tbl_name}: {'; '.join(errors)}")
        return not errors

    def persist(self,
                db_engine: DbEngine = None,
                db_conn: Any = None,
                committable: bool = None,
                errors: list[str] = None) -> bool:

        # declare the return variable
        result: bool

        if self._is_new:
            result = self.insert(db_engine=db_engine,
                                 db_conn=db_conn,
                                 committable=committable,
                                 errors=errors)
        else:
            result = self.update(db_engine=db_engine,
                                 db_conn=db_conn,
                                 committable=committable,
                                 errors=errors)
        return result

    def delete(self,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> int | None:

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        where_data: dict[str, Any]
        tbl_name: str = sob_db_specs[cls_name][0]
        pk_name: str = sob_col_names[cls_name][0]
        if self.id:
            where_data = {pk_name: self.id}
        else:
            where_data = self.get()
            where_data.pop(pk_name, None)

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # execute the DELETE statement
        result: int = db_delete(delete_stmt=f"DELETE FROM {tbl_name}",
                                where_data=where_data,
                                max_count=1,
                                engine=db_engine,
                                connection=db_conn,
                                committable=committable,
                                errors=errors,
                                logger=self._logger)
        if not errors:
            self.clear()
        elif self._logger:
            self._logger.error(msg="Error DELETEing from table "
                                   f"{tbl_name}: {'; '.join(errors)}")
        return result

    def clear(self) -> None:

        for key in self.__dict__:
            self.__dict__[key] = None

    def get(self,
            omit_nulls: bool = True) -> dict[str, Any]:

        # initialize the return variable
        result: dict[str, Any] = {}

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        if not (omit_nulls and self.id is None):
            # PK attribute in DB table might have a different name
            pk_name: str = sob_col_names[cls_name][0]
            result[pk_name] = self.id
        result.update({k: v for k, v in self.__dict__.items()
                      if k.islower() and not (k.startswith("_") or k == "id" or (omit_nulls and v is None))})
        return result

    def set(self,
            data: dict[str, Any]) -> None:

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        tbl_name = sob_db_specs[cls_name][0]
        for key, value in data.items():
            if isinstance(value, Enum) and "use_names" in value.__class__:
                # use enum names assigned as values in 'data'
                value = value.name  # noqa: PLW2901
            if key in self.__dict__:
                self.__dict__[key] = value
            elif self._logger:
                self._logger.warning(msg=f"'{key}' is not an attribute of {tbl_name}")

    def get_inputs(self) -> dict[str, Any] | None:

        # initialize the return variable
        result: dict[str, Any] | None = None

        # obtain the mapping of input names to attributes
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        mapping: list[tuple[str, str]] = sob_attrs_input.get(cls_name)
        if mapping:
            # obtain the instance's non-null data
            data: dict[str, Any] = self.get()
            if data:
                result = dict_clone(source=data,
                                    from_to_keys=[(t[1], t[0]) for t in mapping if t[1]])
        return result

    def is_new(self) -> bool:

        return self._is_new

    def is_in_db(self,
                 db_engine: DbEngine = None,
                 db_conn: Any = None,
                 errors: list[str] = None) -> bool | None:

        # initialize the return variable
        result: bool = False

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        tbl_name: str = sob_db_specs[cls_name][0]
        pk_name: str = sob_col_names[cls_name][0]
        where_data: dict[str, Any] | None = None
        if self.id:
            # use object's ID
            where_data = {pk_name: self.id}
        elif sob_attrs_unique[cls_name]:
            # use first set of unique attributes with non-null values found
            for attr_set in sob_attrs_unique[cls_name]:
                attrs_unique: dict[str, Any] = {}
                for attr in attr_set:
                    val: Any = self.__dict__.get(attr)
                    if val is not None:
                        attrs_unique[attr] = val
                if len(attrs_unique) == len(sob_attrs_unique[cls_name]):
                    where_data = attrs_unique
                    break

        if not where_data:
            # use object's available data
            where_data = self.get()
            where_data.pop(pk_name, None)

        if not where_data:
            result = db_exists(table=tbl_name,
                               where_data=where_data,
                               engine=db_engine,
                               connection=db_conn,
                               errors=errors,
                               logger=self._logger)
        return result

    def load(self,
             __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
             /,
             omit_nulls: bool = True,
             db_engine: DbEngine = None,
             db_conn: Any = None,
             errors: list[str] = None) -> bool:

        # initialize the return variable
        result: bool = False

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        tbl_name: str = sob_db_specs[cls_name][0]
        pk_name: str = sob_col_names[cls_name][0]
        where_data: dict[str, Any]
        if self.id:
            where_data = {pk_name: self.id}
        else:
            where_data = self.get(omit_nulls=omit_nulls)
            where_data.pop(pk_name, None)

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # loading the object from the database might fail
        attrs: list[str] = self.get_columns()
        recs: list[tuple] = db_select(sel_stmt=f"SELECT {', '.join(attrs)} "
                                               f"FROM {tbl_name}",
                                      where_data=where_data,
                                      limit_count=2,
                                      engine=db_engine,
                                      connection=db_conn,
                                      errors=errors,
                                      logger=self._logger)
        msg: str | None = None
        if errors:
            msg = ("Error SELECTing from table "
                   f"{tbl_name}: {'; '.join(errors)}")
        elif not recs:
            msg = (f"No record found on table "
                   f"{tbl_name} for {dict_stringify(where_data)}")
        elif len(recs) > 1:
            msg = (f"More than on record found on table "
                   f"{tbl_name} for {dict_stringify(where_data)}")

        if msg:
            errors.append(msg)
            if self._logger:
                self._logger.error(msg=msg)
        else:
            rec: tuple = recs[0]
            for inx, attr in enumerate(attrs):
                # PK attribute in DB table might have a different name
                if attr == pk_name:
                    self.__dict__["id"] = rec[inx]
                else:
                    self.__dict__[attr] = rec[inx]
            self._is_new = False
            if __references:
                PySob.__load_references(__references,
                                        objs=[self],
                                        db_engine=db_engine,
                                        db_conn=db_conn,
                                        errors=errors)
            result = not errors

        return result

    def get_columns(self) -> list[str]:

        # PK attribute in DB table might have a different name
        return [k for k in self.__dict__
                if k.islower() and not k.startswith("_")]

    # noinspection PyUnusedLocal
    def load_references(self,
                        __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                        /,
                        db_engine: DbEngine = None,
                        db_conn: Any = None,
                        errors: list[str] = None) -> None:

        # must be implemented by subclasses containing references
        msg: str = (f"Subclass {self.__class__.__module__}.{self.__class__.__qualname__} "
                    "failed to implement 'load_references()'")
        if isinstance(errors, list):
            errors.append(msg)
        if self._logger:
            self._logger.error(msg=msg)

    # noinspection PyUnusedLocal
    def invalidate_references(self,
                              __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                              /,
                              db_engine: DbEngine = None,
                              db_conn: Any = None,
                              errors: list[str] = None) -> None:

        # must be implemented by subclasses containing references
        msg: str = (f"Subclass {self.__class__.__module__}.{self.__class__.__qualname__} "
                    "failed to implement 'invalidate_references()'")
        if isinstance(errors, list):
            errors.append(msg)
        if self._logger:
            self._logger.error(msg=msg)

    @staticmethod
    def initialize(db_specs: tuple[type[StrEnum] | list[str], int | str] |
                             tuple[type[StrEnum] | list[str], int, bool],  # noqa
                   attrs_unique: list[tuple[str]] = None,
                   attrs_input: list[tuple[str, str]] = None,
                   logger: Logger = None) -> None:

        # obtain the invoking class
        errors: list[str] = []
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        # initialize its data
        if cls:
            # retrieve the list of DB names
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            attrs: list[str] = [attr.value for attr in db_specs[0]] \
                if isinstance(db_specs[0], type) else db_specs[0].copy()
            tbl_name: str = attrs.pop(0)

            # register the names of DB columnns
            sob_col_names.update({cls_name: tuple(attrs)})

            # register the DB specs (table, PK type, PK entity state)
            if len(db_specs) == 2:
                # PK defaults to being an identity attribute in the DB for type 'int'
                db_specs += (db_specs[1] is int,)
            sob_db_specs.update({cls_name: (tbl_name, db_specs[1], db_specs[2])})

            # register the sets of unique attributes
            if attrs_unique:
                sob_attrs_unique.update({cls_name: attrs_unique})

            # register the names used for data input
            if attrs_input:
                sob_attrs_input.update({cls_name: attrs_input})

            if logger:
                logger.debug(msg=f"Inicialized access data for class '{cls_name}'")

    @staticmethod
    def count(table_alias: str = None,
              table_joins: str = None,
              count_clause: str = None,
              where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              db_engine: DbEngine = None,
              db_conn: Any = None,
              errors: list[str] = None,
              logger: Logger = None) -> int | None:

        # inicialize the return variable
        result: int | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        if not errors:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            tbl_name: str = sob_db_specs[cls_name][0]
            if table_alias:
                tbl_name = f"{tbl_name} {table_alias}"
            if table_joins:
                tbl_name += f" {table_joins}"
            result = db_count(table=tbl_name,
                              count_clause=count_clause,
                              where_clause=where_clause,
                              where_vals=where_vals,
                              where_data=where_data,
                              engine=db_engine,
                              connection=db_conn,
                              errors=errors,
                              logger=logger)
        if errors and logger:
            logger.error(msg="; ".join(errors))

        return result

    @staticmethod
    def exists(table_alias: str = None,
               table_joins: str = None,
               where_clause: str = None,
               where_vals: tuple = None,
               where_data: dict[str, Any] = None,
               min_count: int = None,
               max_count: int = None,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               errors: list[str] = None,
               logger: Logger = None) -> int | None:

        # inicialize the return variable
        result: bool | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        if not errors:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            tbl_name: str = sob_db_specs[cls_name][0]
            if table_alias:
                tbl_name = f"{tbl_name} {table_alias}"
            if table_joins:
                tbl_name += f" {table_joins}"
            result = db_exists(table=tbl_name,
                               where_clause=where_clause,
                               where_vals=where_vals,
                               where_data=where_data,
                               min_count=min_count,
                               max_count=max_count,
                               engine=db_engine,
                               connection=db_conn,
                               errors=errors,
                               logger=logger)
        if errors and logger:
            logger.error(msg="; ".join(errors))

        return result

    @staticmethod
    def get_values(attrs: list[str],
                   table_alias: str = None,
                   table_joins: str = None,
                   where_clause: str = None,
                   where_vals: tuple = None,
                   where_data: dict[str, Any] = None,
                   db_engine: DbEngine = None,
                   db_conn: Any = None,
                   committable: bool = None,
                   errors: list[str] = None,
                   logger: Logger = None) -> list[tuple] | None:

        # inicialize the return variable
        result: list[tuple] | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        if not errors:
            # build the attributes list
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            tbl_name: str = sob_db_specs[cls_name][0]
            if table_alias:
                tbl_name = f"{tbl_name} {table_alias}"
            if table_joins:
                tbl_name += f" {table_joins}"
            result = db_select(sel_stmt=f"SELECT DISTINCT {''.join(attrs)} FROM {tbl_name}",
                               where_clause=where_clause,
                               where_vals=where_vals,
                               where_data=where_data,
                               engine=db_engine,
                               connection=db_conn,
                               committable=committable,
                               errors=errors,
                               logger=logger)
        return result

    @staticmethod
    def retrieve(__references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
                 /,
                 table_alias: str = None,
                 table_joins: str = None,
                 where_clause: str = None,
                 where_vals: tuple = None,
                 where_data: dict[str, Any] = None,
                 orderby_clause: str = None,
                 min_count: int = None,
                 max_count: int = None,
                 offset_count: int = None,
                 limit_count: int = None,
                 db_engine: DbEngine = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None,
                 logger: Logger = None) -> list[Sob] | None:

        # inicialize the return variable
        result: list[Sob] | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        if not errors:
            # build the attributes list
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            tbl_name: str = sob_db_specs[cls_name][0]
            attrs: list[str] = []
            for attr in sob_col_names.get(cls_name):
                if table_alias:
                    attr = f"{table_alias}.{attr}"
                attrs.append(attr)
            if table_alias:
                tbl_name = f"{tbl_name} {table_alias}"
            if table_joins:
                tbl_name += f" {table_joins}"
            sel_stmt: str = f"SELECT DISTINCT {', '.join(attrs)} FROM {tbl_name}"
            recs: list[tuple[int | str]] = db_select(sel_stmt=sel_stmt,
                                                     where_clause=where_clause,
                                                     where_vals=where_vals,
                                                     where_data=where_data,
                                                     orderby_clause=orderby_clause,
                                                     min_count=min_count,
                                                     max_count=max_count,
                                                     offset_count=offset_count,
                                                     limit_count=limit_count,
                                                     engine=db_engine,
                                                     connection=db_conn,
                                                     committable=committable,
                                                     errors=errors,
                                                     logger=logger)
            if not errors:
                # build the objects list
                objs: list[Sob] = []
                for rec in recs:
                    data: dict[str, Any] = {}
                    for inx, attr in enumerate(sob_col_names.get(cls_name)):
                        data[attr] = rec[inx]
                    sob: type[Sob] = cls()
                    sob.set(data=data)
                    sob._is_new = False
                    if errors:
                        break
                    objs.append(sob)

                if __references:
                    PySob.__load_references(__references,
                                            objs=objs,
                                            db_engine=db_engine,
                                            db_conn=db_conn,
                                            errors=errors)
                if not errors:
                    result = objs

        if errors and logger:
            logger.error(msg="; ".join(errors))

        return result

    @staticmethod
    def store(insert_data: dict[str, Any] = None,
              return_cols: dict[str, Any] = None,
              db_engine: DbEngine = None,
              db_conn: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> tuple | int | None:

        # initialize the return variable
        result: tuple | int | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        # delete specified rows
        if not errors:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            tbl_name: str = sob_db_specs[cls_name][0]
            result = db_insert(insert_stmt=f"INSERT INTO {tbl_name}",
                               insert_data=insert_data,
                               return_cols=return_cols,
                               engine=db_engine,
                               connection=db_conn,
                               committable=committable,
                               errors=errors,
                               logger=logger)
        if errors and logger:
            logger.error(msg="; ".join(errors))

        return result

    @staticmethod
    def erase(where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              min_count: int = None,
              max_count: int = None,
              db_engine: DbEngine = None,
              db_conn: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> int | None:

        # initialize the return variable
        result: int | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        # delete specified rows
        if not errors:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            tbl_name: str = sob_db_specs[cls_name][0]
            result = db_delete(delete_stmt=f"DELETE FROM {tbl_name}",
                               where_clause=where_clause,
                               where_vals=where_vals,
                               where_data=where_data,
                               min_count=min_count,
                               max_count=max_count,
                               engine=db_engine,
                               connection=db_conn,
                               committable=committable,
                               errors=errors,
                               logger=logger)
        if errors and logger:
            logger.error(msg="; ".join(errors))

        return result

    @staticmethod
    def __get_invoking_class(errors: list[str] | None,
                             logger: Logger | None) -> type[Sob] | None:

        # initialize the return variable
        result: type[Sob] | None = None

        # obtain the invoking function
        caller_frame: FrameInfo = stack()[1]
        invoking_function: str = caller_frame.function
        mark: str = f".{invoking_function}("

        # obtain the invoking class and its filepath
        caller_frame = stack()[2]
        context: str = caller_frame.code_context[0]
        pos_to: int = context.find(mark)
        pos_from: int = context.rfind(" ", 0, pos_to) + 1
        classname: str = context[pos_from:pos_to]
        filepath: Path = Path(caller_frame.filename)
        mark = "." + classname

        for name in sob_db_specs:
            if name.endswith(mark):
                try:
                    pos: int = name.rfind(".")
                    module_name: str = name[:pos]
                    module: ModuleType = import_module(name=module_name)
                    result = getattr(module,
                                     classname)
                except Exception as e:
                    if logger:
                        msg: str = exc_format(exc=e,
                                              exc_info=sys.exc_info())
                        logger.warning(msg=msg)
                break

        if not result and SOB_BASE_FOLDER:
            try:
                pos: int = filepath.parts.index(SOB_BASE_FOLDER)
                module_name: str = Path(*filepath.parts[pos:]).as_posix()[:-3].replace("/", ".")
                module: ModuleType = import_module(name=module_name)
                result = getattr(module,
                                 classname)
            except Exception as e:
                if logger:
                    msg: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                    logger.warning(msg=msg)

        if not result:
            msg: str = (f"Unable to obtain class '{classname}', "
                        f"filepath '{filepath}', "f"from invoking function '{invoking_function}'")
            if logger:
                logger.error(msg=f"{msg} - invocation frame {caller_frame}")
            if isinstance(errors, list):
                errors.append(msg)

        return result

    @staticmethod
    def __load_references(__references:  type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                          /,
                          objs: list[Sob],
                          db_engine: DbEngine | None,
                          db_conn: Any | None,
                          errors: list[str]) -> None:

        if SOB_MAX_THREADS > 1 and \
                (len(objs) > 1 or (isinstance(__references, list) and len(__references) > 1)):
            task_futures: list[Future] = []
            with ThreadPoolExecutor(max_workers=SOB_MAX_THREADS) as executor:
                for obj in objs:
                    for reference in __references if isinstance(__references, list) else [__references]:
                        # must not multiplex 'db_conn'
                        future: Future = executor.submit(obj.load_references,
                                                         reference,
                                                         db_engine=db_engine,
                                                         errors=errors)
                        if errors:
                            break
                        task_futures.append(future)
                    if errors:
                        break

            # wait for all task futures to complete, then shutdown down the executor
            futures.wait(fs=task_futures)
            executor.shutdown(wait=False)
        else:
            for obj in objs:
                obj.load_references(__references,
                                    db_engine=db_engine,
                                    db_conn=db_conn,
                                    errors=errors)
                if errors:
                    break
