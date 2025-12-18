from __future__ import annotations  # allow forward references
import sys
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from enum import StrEnum
from importlib import import_module
from inspect import FrameInfo, stack
from enum import Enum, IntEnum
from logging import Logger
from pathlib import Path
from pypomes_core import (
    StrEnumUseName,
    dict_clone, dict_has_value, dict_stringify, exc_format
)
from pypomes_db import (
    DbEngine, db_exists, db_count,
    db_select, db_insert, db_update, db_delete
)
from types import ModuleType
from typing import Any, Literal, TypeVar

from .sob_config import (
    SOB_BASE_FOLDER, SOB_MAX_THREADS,
    sob_db_columns, sob_db_specs,
    sob_attrs_input, sob_attrs_unique, sob_loggers
)

# 'Sob' stands for all subclasses of 'PySob'
Sob = TypeVar("Sob",
              bound="PySob")


class PySob:
    """
    Root entity for the *SOB* (Simple object) hierarchy.

    The *SOB* objects are mapped to a *RDBMS* table, and present relationships within and among themselves
    typical of the relational paradigm, such as primary and foreign keys, nullability, uniqueness, one-to-many,
    and many-to-many, to mention just a few.

    The only instance attribute defined at root class level is *id* (type *int* or *str*), the object's
    identification, which may be mapped to a different name in storage. Other attributes are expected to be
    defined by its subclasses.
    """

    def __init__(self,
                 __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
                 /,
                 where_data: dict[str, Any] = None,
                 db_engine: DbEngine = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:
        """
        Instantiate a *SOB* object from its subclass.

        If loading the corresponding data from storage is desired, *where_data* should be specifed,
        and the criteria therein should yield just one tuple, when used in a *SELECT* statement.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        If provided, *logger* is used, and saved for further usage in operations involving the object instances.

        :param __references: the *SOB* references to load at object instantiation time
        :param where_data: the criteria to load the object's data from the database
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        """
        # maps to the entity's PK in its DB table (returned on INSERT operations)
        self.id: int | str | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        if where_data:
            self.set(data=where_data)
            self.load(__references,
                      omit_nulls=True,
                      db_engine=db_engine,
                      db_conn=db_conn,
                      committable=committable,
                      errors=errors)

    def insert(self,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool:
        """
        Attempt to persist the current state of the object in the database with an *INSERT* operation.

        If the primary key represents an identity column (that is, its contents are handled by the
        database at insert time), then the value assigned to it be the database is assigned to the
        corresponding attribute.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: *True* if the operation was successful, or *False* otherwise
        """
        # prepare data for INSERT
        return_col: dict[str, type] | None = None
        insert_data: dict[str, Any] = self.get()
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        is_identity: bool = sob_db_specs[cls_name][2]
        if is_identity:
            # PK is an identity column
            pk_name: str = sob_db_columns[cls_name][0]
            pk_type: type = sob_db_specs[cls_name][1]
            insert_data.pop(pk_name, None)
            return_col = {pk_name: pk_type}

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # execute the INSERT statement
        logger: Logger = sob_loggers.get(cls_name)
        tbl_name = sob_db_specs[cls_name][0]
        rec: tuple[Any] = db_insert(insert_stmt=f"INSERT INTO {tbl_name}",
                                    insert_data=insert_data,
                                    return_cols=return_col,
                                    engine=db_engine,
                                    connection=db_conn,
                                    committable=committable,
                                    errors=errors,
                                    logger=logger)
        if rec is not None:
            if is_identity:
                # PK is an identity column
                self.id = rec[0]
        elif logger:
            logger.error(msg="Error INSERTing into table "
                             f"{tbl_name}: {'; '.join(errors)}")
        return not errors

    def update(self,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool:
        """
        Attempt to persist the current state of the object in the database with an *UPDATE* operation.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: *True* if the operation was successful, or *False* otherwise
        """
        # prepare data for UPDATE
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        pk_name: str = sob_db_columns[cls_name][0]
        tbl_name: str = sob_db_specs[cls_name][0]
        update_data: dict[str, Any] = self.get(omit_nulls=False)
        key: int | str = update_data.pop(pk_name)

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # execute the UPDATE statement
        logger: Logger = sob_loggers.get(cls_name)
        return db_update(update_stmt=f"UPDATE {tbl_name}",
                         update_data=update_data,
                         where_data={pk_name: key},
                         min_count=1,
                         max_count=1,
                         engine=db_engine,
                         connection=db_conn,
                         committable=committable,
                         errors=errors,
                         logger=logger) is not None

    def persist(self,
                db_engine: DbEngine = None,
                db_conn: Any = None,
                committable: bool = None,
                errors: list[str] = None) -> bool:
        """
        Attempt to persist the current state of the object in the database with the appropriate operation.

        The operation to be performed will depend on whether the object's identification in the database
        (its *id attribute) has been set or not (yielding respectively, *UPDATE* or *INSERT*).

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: *True* if the operation was successful, or *False* otherwise
        """
        # declare the return variale
        result: bool

        if self.id:
            result = self.update(db_engine=db_engine,
                                 db_conn=db_conn,
                                 committable=committable,
                                 errors=errors)
        else:
            result = self.insert(db_engine=db_engine,
                                 db_conn=db_conn,
                                 committable=committable,
                                 errors=errors)
        return result

    def delete(self,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> int | None:
        """
        Attempt to remove the object from the database with a *DELETE* operation.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: the number of deleted tuples, or *None* if error
        """
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        where_data: dict[str, Any]
        pk_name: str = sob_db_columns[cls_name][0]
        tbl_name: str = sob_db_specs[cls_name][0]
        if self.id:
            where_data = {pk_name: self.id}
        else:
            where_data = self.get()
            where_data.pop(pk_name, None)

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # execute the DELETE statement
        logger: Logger = sob_loggers.get(cls_name)
        result: int = db_delete(delete_stmt=f"DELETE FROM {tbl_name}",
                                where_data=where_data,
                                max_count=1,
                                engine=db_engine,
                                connection=db_conn,
                                committable=committable,
                                errors=errors,
                                logger=logger)
        if result is not None:
            self.clear()

        return result

    def clear(self) -> None:
        """
        Set all of the object's attributes to *None*.

        This should be of very infrequent use, if any, and thus extreme care should be exercised.
        """
        for key in self.__dict__:
            self.__dict__[key] = None

    def get(self,
            omit_nulls: bool = True) -> dict[str, Any]:
        """
        Retrieve the names and current values of all the object's attributes, and return them in a *dict*.

        Note that only the public attributes are returned. Attributes starting with '_' (*underscore*) are omitted.

        :param omit_nulls: whether to include the attributes with null values
        :return: key/value pairs of the names and current values of the object's public attributes
        """
        # initialize the return variable
        result: dict[str, Any] = {}

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        if not (omit_nulls and self.id is None):
            # PK attribute in DB table might have a different name
            pk_name: str = sob_db_columns[cls_name][0]
            result[pk_name] = self.id
        result.update({k: v for k, v in self.__dict__.items()
                      if k.islower() and not (k.startswith("_") or k == "id" or (omit_nulls and v is None))})
        return result

    def set(self,
            data: dict[str, Any]) -> None:
        """
        Set the values of the object's attributes as per *data*.

        Keys in *data* not corresponding to actual object's attributes are ignored.

        :param data: key/value pairs to set the object with
        """
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        logger: Logger = sob_loggers.get(cls_name)

        # traverse the data
        for ky, val in data.items():

            # normalize 'ky'
            if isinstance(ky, Enum):
                if isinstance(ky, IntEnum | StrEnumUseName):
                    ky = ky.name.lower()
                else:
                    # HAZARD: values of 'IntEnum' cannot be used as attribute names
                    ky = ky.value.lower()

            # normalize 'val'
            if isinstance(val, Enum):
                if isinstance(val, StrEnumUseName):
                    val = val.name
                else:
                    val = val.value

            # register the key/value pair
            if ky in self.__dict__:
                self.__dict__[ky] = val
            elif logger:
                logger.warning(msg=f"'{ky}' is not an attribute of class {cls_name}")

    def get_inputs(self) -> dict[str, Any] | None:
        """
        Retrieve the input names and current values of the object's attributes, and return them in a *dict*.

        Input names are the names used for the object's attributes on input operations. The mapping of the
        input names to actual names must have been done at the class' initalization time, with the appropriate
        parameter in the *initialize()* operation. If this optional mapping has not been done, *None* is returned.

        :return: key/value pairs of the object's input names and current values, or *None* if not mapped
        """
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

    def is_persisted(self,
                     db_engine: DbEngine = None,
                     db_conn: Any = None,
                     committable: bool = None,
                     errors: list[str] = None) -> bool | None:
        """
        Attempt to determine if the current state of the object is persisted in the database.

        These are the sequence of steps to follow:
            - use the object's identification in the database (its *id* attribute), if set
            - use the first set of *unique* attributes found with non-null values
            - use all the object's public attributes with non-null values

        The optional sets of *unique* attributes are specifed at class initialization time, with the
        appropriate parameter in the *initialize()* operation.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: *True* the object's current state is persisted, *False* otherwise, or *None* if error
        """
        # initialize the return variable
        result: bool = False

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        tbl_name: str = sob_db_specs[cls_name][0]

        # build the WHERE clause
        where_data: dict[str, Any] = self.get_unique_attrs()
        if not where_data:
            # use object's available data
            where_data = self.get()

        # execute the query
        if where_data:
            logger: Logger = sob_loggers.get(cls_name)
            result = db_exists(table=tbl_name,
                               where_data=where_data,
                               engine=db_engine,
                               connection=db_conn,
                               committable=committable,
                               errors=errors,
                               logger=logger)
        return result

    def load(self,
             __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
             /,
             omit_nulls: bool = True,
             db_engine: DbEngine = None,
             db_conn: Any = None,
             committable: bool = None,
             errors: list[str] = None) -> bool:
        """
        Set the current state of the object by loading the corresponding data from the database.

        These are the sequence of steps to follow:
            - use the object's identification in the database (its *id* attribute), if set
            - use the first set of *unique* attributes found with non-null values
            - use all the object's public attributes with non-null values

        The optional sets of *unique* attributes are specifed at class initialization time, with the
        appropriate parameter in the *initialize()* operation.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param __references: the *SOB* references to load
        :param omit_nulls: whether to include the attributes with null values
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: *True* if the operation was successful, or *False* otherwise
        """
        # initialize the return variable
        result: bool = False

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        tbl_name: str = sob_db_specs[cls_name][0]

        # build the WHERE clause
        where_data: dict[str, Any] = self.get_unique_attrs()
        if not where_data:
            # use object's available data
            where_data = self.get(omit_nulls=omit_nulls)

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # loading the object from the database might fail
        logger: Logger = sob_loggers.get(cls_name)
        attrs: list[str] = self.get_columns()
        recs: list[tuple] = db_select(sel_stmt=f"SELECT {', '.join(attrs)} FROM {tbl_name}",
                                      where_data=where_data,
                                      limit_count=2,
                                      engine=db_engine,
                                      connection=db_conn,
                                      committable=committable,
                                      errors=errors,
                                      logger=logger)
        if recs is not None:
            msg: str | None = None
            if len(recs) == 0:
                msg = "No record"
            elif len(recs) > 1:
                msg = "More than one record"
            if msg:
                msg += f" found on table {tbl_name} for {dict_stringify(where_data)}"
                errors.append(msg)
                if logger:
                    logger.error(msg=msg)

        if not errors:
            pk_name: str = sob_db_columns[cls_name][0]
            rec: tuple = recs[0]
            for inx, attr in enumerate(attrs):
                # PK attribute in DB table might have a different name
                if attr == pk_name:
                    self.__dict__["id"] = rec[inx]
                else:
                    self.__dict__[attr] = rec[inx]
            if __references:
                result = PySob.__load_references(__references,
                                                 objs=[self],
                                                 db_engine=db_engine,
                                                 db_conn=db_conn,
                                                 committable=committable,
                                                 errors=errors) is not None
        return result

    def get_columns(self) -> list[str]:
        """
        Retrieve the names of the object's public attributes as mapped to its corresponding database table.

        :return: a list with the names of the object's attributes as mapped to a database table.
        """
        # PK attribute in DB table might have a different name
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        pk_name: str = sob_db_columns[cls_name][0]

        result: list[str] = []
        for key in self.__dict__:
            if key == "id":
                result.append(pk_name)
            elif key.islower() and not key.startswith("_"):
                result.append(key)

        return result

    def get_unique_attrs(self) -> dict[str, Any]:
        """
        Retrieve the key/value pairs of the first set of *unique* attributes with non-null values found.

        The first attribute set to check is the default, which is comprised of the object's *id* attribute.
        The optional sets of *unique* attributes are specifed at class initialization time, with the
        appropriate parameter in the *initialize()* operation.

        :return: a *dict* with key/value pairs of the first set of *unique* attributes with non-null values found
        """
        # initialize the return variable
        result: dict[str, Any] = {}

        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        if self.id:
            # use the object's 'id' attribute
            pk_name: str = sob_db_columns[cls_name][0]
            result = {pk_name: self.id}
        elif cls_name in sob_attrs_unique:
            # use first set of unique attributes found with non-null values
            for attr_set in sob_attrs_unique[cls_name]:
                data: dict[str, Any] = {}
                for attr in attr_set:
                    val: Any = self.__dict__.get(attr)
                    if val is not None:
                        data[attr] = val
                if len(data) == len(attr_set):
                    result = data
                    break

        return result

    # noinspection PyUnusedLocal
    # ruff: noqa: ARG002
    def load_references(self,
                        __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                        /,
                        db_engine: DbEngine = None,
                        db_conn: Any = None,
                        committable: bool = None,
                        errors: list[str] = None) -> None:
        """
        Load the *SOB* references specified in *__references* from the database.

        This operation must be implemented at the subclass level.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param __references: the *SOB* references to load
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        """
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        logger: Logger = sob_loggers.get(cls_name)

        # must be implemented by subclasses containing references
        msg: str = (f"Subclass {self.__class__.__module__}.{self.__class__.__qualname__} "
                    "failed to implement 'load_references()'")
        if isinstance(errors, list):
            errors.append(msg)
        if logger:
            logger.error(msg=msg)

    # noinspection PyUnusedLocal
    # ruff: noqa: ARG002
    def invalidate_references(self,
                              __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                              /,
                              db_engine: DbEngine = None,
                              db_conn: Any = None,
                              committable: bool = None,
                              errors: list[str] = None) -> None:
        """
        Invalidate the object's *SOB* references specified in *__references*.

        Whenever needed, this operation must be implemented at the subclass level.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param __references: the *SOB* references to invalidate
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        """
        cls_name: str = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        logger: Logger = sob_loggers.get(cls_name)

        # must be implemented by subclasses containing references
        msg: str = (f"Subclass {self.__class__.__module__}.{self.__class__.__qualname__} "
                    "failed to implement 'invalidate_references()'")
        if isinstance(errors, list):
            errors.append(msg)
        if logger:
            logger.error(msg=msg)

    # noinspection PyPep8
    @staticmethod
    def initialize(db_specs: tuple[type[StrEnum] | list[str], type[int | str]] |
                             tuple[type[StrEnum] | list[str], type[int], bool],
                   attrs_unique: list[tuple[str]] = None,
                   attrs_input: list[tuple[str, str]] = None,
                   logger: Logger = None) -> None:
        """
        Initialize the subclass with required and optional data.

        The parameter *db_specs* is a tuple with two or three elements:
            - list of names used in the class' corresponding database table:
                - the first item is the table name
                - the second itwm is the name of the table's primary key column (mapped to the *id* attribute)
                - the remaining items are the column names
            - the type of the primary key (*int* or *str*)
            - if the primary key's type is *int*, whether it is an identity column (defaults to *True*)

        The optional parameter *attrs_unique* is a list of tuples, each one containing a set of one or more
        attributes whose values guarantee uniqueness in the database table.

        The optional parameter *attrs_input* maps names used in input operations to the actual names of the
        attributes. It is not required that it be complete, as it might map only a subset of attributes.

        If provided, *logger* is used, and saved for further usage in operations involving the class and
        object instances.

        :param db_specs: the attributes mapped to the corresponding database table
        :param attrs_unique: the sets of attributes defining object's uniqueness in the database
        :param attrs_input: mapping of names used in input to actual attribute names
        :param logger: optional logger
        """
        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(logger=logger)
        # initialize its data
        if cls:
            # retrieve the list of DB names
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            attrs: list[str] = [attr.value for attr in db_specs[0]] \
                if isinstance(db_specs[0], type) else db_specs[0].copy()
            tbl_name: str = attrs.pop(0)

            # register the names of DB columnns
            sob_db_columns.update({cls_name: tuple(attrs)})

            # register the DB specs (table, PK type, PK entity state)
            if len(db_specs) == 2:
                # PK defaults to being an identity attribute in the DB for type 'int'
                db_specs += (db_specs[1] is int,)
            sob_db_specs[cls_name] = (tbl_name, db_specs[1], db_specs[2])

            # register the sets of unique attributes
            if attrs_unique:
                sob_attrs_unique.update({cls_name: attrs_unique})

            # register the names used for data input
            if attrs_input:
                sob_attrs_input.update({cls_name: attrs_input})

            if logger:
                sob_loggers[cls_name] = logger
                logger.debug(msg=f"Inicialized access data for class {cls_name}")

    @staticmethod
    def get_logger() -> Logger | None:
        """
        Get the logger associated with the current subclass.

        :return: the logger associated with the subclass, of *None* if error or not provided
        """
        # initialize the return variable
        result: Logger | None = None

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class()

        if cls:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            result = sob_loggers.get(cls_name)

        return result

    @staticmethod
    def count(joins: list[tuple[type[Sob],
                                list[tuple[StrEnum, StrEnum,
                                           Literal["=", "<>", "<=", ">="] | None,
                                           Literal["and", "or"] | None]],
                                Literal["inner", "full", "left", "right"] | None]] | list[tuple] = None,
              count_clause: str = None,
              where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[StrEnum | tuple |
                               tuple[StrEnum,
                                     Literal["=", ">", "<", ">=", "<=",
                                             "<>", "in", "like", "between"] | None,
                                     Literal["and", "or"] | None], Any] = None,
              db_engine: DbEngine = None,
              db_conn: Any = None,
              committable: bool = None,
              errors: list[str] = None) -> int | None:
        """
        Count the occurrences of tuples in the corresponding database table, as per the criteria provided.

        Optionally, *joins* holds a list of tuples specifying the table joins, with the following format:
            1. the first element in the tuple identifies the table:
                - a singlet, with the type of the *Sob* subclass whose database table is to be joined
            2. a 2/3/4-tuple, or a list of 2/3/4-tuples, informs on the *ON* fragment conditions:
                - the first attribute, as a string or a *StrEnum* instance
                - the second attribute, as a string or a *StrEnum* instance
                - the operation ("=", "<>", "<=", or ">=", defaults to "=")
                - the connector to the next condition ("and" or "or", defaults to "and")
            3. the third element is the type of the join ("inner", "full", "left", "right", defaults to "inner")

        Optionally, selection criteria may be specified in *where_clause* and *where_vals*, or additionally but
        preferably, by key-value pairs in *where_data*. Care should be exercised if *where_clause* contains *IN*
        directives. In PostgreSQL, the list of values for an attribute with the *IN* directive must be contained
        in a specific tuple, and the operation will break for a list of values containing only 1 element.
        The safe way to specify *IN* directives is to add them to *where_data*, as the specifics of each DB flavor
        will then be properly dealt with.

        The syntax specific to *where_data*'s key/value pairs is as follows:
            1. *key*:
                - an attribute (*StrEnum*, or *str* possibly aliased), or
                - a 2/3-tuple with an attribute and the corresponding SQL comparison operation
                  ("=", ">", "<", ">=", "<=", "<>", "in", "like", "between" - defaults to "="), followed
                  by a SQL logical operator relating it to the next item ("and", "or" - defaults to "and")
            2. *value*:
                - a scalar, or a list, or an expression possibly containing other attribute(s)

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param joins: optional *JOIN* clauses to use
        :param count_clause: optional parameters in the *COUNT* clause (defaults to 'COUNT(*)')
        :param where_clause: optional criteria for tuple selection
        :param where_vals: values to be associated with the selection criteria
        :param where_data: the selection criteria specified as key-value pairs
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: the number of tuples counted, or *None* if error
        """
        # inicialize the return variable
        result: int | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        if cls:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            logger: Logger = sob_loggers.get(cls_name)

            # obtain the aliases map
            aliases: dict[str, str] = PySob.__build_aliases_map(subcls=cls,
                                                                joins=joins)
            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(subcls=cls,
                                                         aliases=aliases,
                                                         joins=joins)
            # retrieve the data
            where_data = PySob.__add_aliases(attrs=where_data,
                                             aliases=aliases)
            result = db_count(table=from_clause,
                              count_clause=count_clause,
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
    def exists(joins: list[tuple[type[Sob],
                                 list[tuple[StrEnum, StrEnum,
                                            Literal["=", "<>", "<=", ">="] | None,
                                            Literal["and", "or"] | None]],
                                 Literal["inner", "full", "left", "right"] | None]] | list[tuple] = None,
               where_clause: str = None,
               where_vals: tuple = None,
               where_data: dict[StrEnum | tuple |
                                tuple[StrEnum,
                                      Literal["=", ">", "<", ">=", "<=",
                                              "<>", "in", "like", "between"] | None,
                                      Literal["and", "or"] | None], Any] = None,
               min_count: int = None,
               max_count: int = None,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool | None:
        """
        Determine if at least one tuple exists in the database table, as per the criteria provided.

        Optionally, *joins* holds a list of tuples specifying the table joins, with the following format:
            1. the first element in the tuple identifies the table:
                - a singlet, with the type of the *Sob* subclass whose database table is to be joined
            2. a 2/3/4-tuple, or a list of 2/3/4-tuples, informs on the *ON* fragment conditions:
                - the first attribute, as a string or a *StrEnum* instance
                - the second attribute, as a string or a *StrEnum* instance
                - the operation ("=", "<>", "<=", or ">=", defaults to "=")
                - the connector to the next condition ("and" or "or", defaults to "and")
            3. the third element is the type of the join ("inner", "full", "left", "right", defaults to "inner")

        Optionally, selection criteria may be specified in *where_clause* and *where_vals*, or additionally but
        preferably, by key-value pairs in *where_data*. Care should be exercised if *where_clause* contains *IN*
        directives. In PostgreSQL, the list of values for an attribute with the *IN* directive must be contained
        in a specific tuple, and the operation will break for a list of values containing only 1 element.
        The safe way to specify *IN* directives is to add them to *where_data*, as the specifics of each DB flavor
        will then be properly dealt with.

        The syntax specific to *where_data*'s key/value pairs is as follows:
            1. *key*:
                - an attribute (*StrEnum*, or *str* possibly aliased), or
                - a 2/3-tuple with an attribute and the corresponding SQL comparison operation
                  ("=", ">", "<", ">=", "<=", "<>", "in", "like", "between" - defaults to "="), followed
                  by a SQL logical operator relating it to the next item ("and", "or" - defaults to "and")
            2. *value*:
                - a scalar, or a list, or an expression possibly containing other attribute(s)

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param joins: optional *JOIN* clauses to use
        :param where_clause: optional criteria for tuple selection
        :param where_vals: values to be associated with the selection criteria
        :param where_data: the selection criteria specified as key-value pairs
        :param min_count: optionally defines the minimum number of tuples expected to exist
        :param max_count: optionally defines the maximum number of tuples expected to exist
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: *True* if the criteria for tuple existence were met, *False* otherwise, or *None* if error
        """
        # inicialize the return variable
        result: bool | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        if cls:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            logger: Logger = sob_loggers.get(cls_name)

            # obtain the aliases map
            aliases: dict[str, str] = PySob.__build_aliases_map(subcls=cls,
                                                                joins=joins)
            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(subcls=cls,
                                                         aliases=aliases,
                                                         joins=joins)
            # execute the query
            where_data = PySob.__add_aliases(attrs=where_data,
                                             aliases=aliases)
            result = db_exists(table=from_clause,
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
        return result

    # noinspection PyPep8
    @staticmethod
    def get_values(attrs: tuple[str],
                   joins: list[tuple[type[Sob],
                                     list[tuple[StrEnum, StrEnum,
                                                Literal["=", "<>", "<=", ">="] | None,
                                                Literal["and", "or"] | None]],
                                     Literal["inner", "full", "left", "right"] | None]] | list[tuple] = None,
                   where_clause: str = None,
                   where_vals: tuple = None,
                   where_data: dict[StrEnum | tuple |
                                    tuple[StrEnum,
                                          Literal["=", ">", "<", ">=", "<=",
                                                  "<>", "in", "like", "between"] | None,
                                          Literal["and", "or"] | None], Any] = None,
                   orderby_clause: StrEnum |
                                   tuple[StrEnum, Literal["asc", "desc"] | None] |
                                   list[StrEnum | tuple[StrEnum, Literal["asc", "desc"] | None]] = None,
                   min_count: int = None,
                   max_count: int = None,
                   offset_count: int = None,
                   limit_count: int = None,
                   db_engine: DbEngine = None,
                   db_conn: Any = None,
                   committable: bool = None,
                   errors: list[str] = None) -> list[tuple] | None:
        """
        Retrieve the values of *attrs* from the database, as per the criteria provided.

        Optionally, *joins* holds a list of tuples specifying the table joins, with the following format:
            1. the first element in the tuple identifies the table:
                - a singlet, with the type of the *Sob* subclass whose database table is to be joined
            2. a 2/3/4-tuple, or a list of 2/3/4-tuples, informs on the *ON* fragment conditions:
                - the first attribute, as a string or a *StrEnum* instance
                - the second attribute, as a string or a *StrEnum* instance
                - the operation ("=", "<>", "<=", or ">=", defaults to "=")
                - the connector to the next condition ("and" or "or", defaults to "and")
            3. the third element is the type of the join ("inner", "full", "left", "right", defaults to "inner")

        Optionally, selection criteria may be specified in *where_clause* and *where_vals*, or additionally but
        preferably, by key-value pairs in *where_data*. Care should be exercised if *where_clause* contains *IN*
        directives. In PostgreSQL, the list of values for an attribute with the *IN* directive must be contained
        in a specific tuple, and the operation will break for a list of values containing only 1 element.
        The safe way to specify *IN* directives is to add them to *where_data*, as the specifics of each DB flavor
        will then be properly dealt with.

        The syntax specific to *where_data*'s key/value pairs is as follows:
            1. *key*:
                - an attribute (*StrEnum*, or *str* possibly aliased), or
                - a 2/3-tuple with an attribute and the corresponding SQL comparison operation
                  ("=", ">", "<", ">=", "<=", "<>", "in", "like", "between" - defaults to "="), followed
                  by a SQL logical operator relating it to the next item ("and", "or" - defaults to "and")
            2. *value*:
                - a scalar, or a list, or an expression possibly containing other attribute(s)

        The sort order of the query results may be specified by *orderby_clause*, which might be:
            - a *str* or *StrEnum* indicating the attribute and the default sort direction *asc*, or
            - a tuple or a list of tuples, each indicating an attribute and its sort direction (defaults to *asc*)

        If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
        If both *min_count* and *max_count* are specified with equal values, then exactly that number of
        tuples must be returned by the query. The parameter *offset_count* is used to offset the retrieval
        of tuples. For both *offset_count* and *limit_count* to be used together with SQLServer, an *ORDER BY*
        clause must have been specifed, otherwise a runtime error is raised.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param attrs: one or more attributes whose values to retrieve
        :param joins: optional *JOIN* clauses to use
        :param where_clause: optional criteria for tuple selection
        :param where_vals: values to be associated with the selection criteria
        :param where_data: the selection criteria specified as key-value pairs
        :param orderby_clause: optional retrieval order
        :param min_count: optionally defines the minimum number of tuples expected to be retrieved
        :param max_count: optionally defines the maximum number of tuples expected to be retrieved
        :param offset_count: number of tuples to skip (defaults to none)
        :param limit_count: limit to the number of tuples returned, to be specified in the query statement itself
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: a list containing tuples with the values retrieved, *[]* on empty result, or *None* if error
        """
        # inicialize the return variable
        result: list[tuple] | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        if cls:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            logger: Logger = sob_loggers.get(cls_name)

            # obtain the aliases map
            aliases: dict[str, str] = PySob.__build_aliases_map(subcls=cls,
                                                                joins=joins)
            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(subcls=cls,
                                                         aliases=aliases,
                                                         joins=joins)
            # build the aliased attributes list
            aliased_attrs: list[str] = PySob.__add_aliases(attrs=list(attrs),
                                                           aliases=aliases)
            # normalize the 'ORDER BY' clause
            if orderby_clause:
                orderby_clause = PySob.__normalize_orderby(orderby=orderby_clause,
                                                           aliases=aliases)
            # retrieve the data
            sel_stmt: str = f"SELECT DISTINCT {', '.join(aliased_attrs)} FROM {from_clause}"
            result = db_select(sel_stmt=sel_stmt,
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
        return result

    @staticmethod
    def get_single(__references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
                   /,
                   joins: list[tuple[type[Sob],
                                     list[tuple[StrEnum, StrEnum,
                                                Literal["=", "<>", "<=", ">="] | None,
                                                Literal["and", "or"] | None]],
                                     Literal["inner", "full", "left", "right"] | None]] | list[tuple] = None,
                   where_clause: str = None,
                   where_vals: tuple = None,
                   where_data: dict[StrEnum | tuple |
                                    tuple[StrEnum,
                                          Literal["=", ">", "<", ">=", "<=",
                                                  "<>", "in", "like", "between"] | None,
                                          Literal["and", "or"] | None], Any] = None,
                   db_engine: DbEngine = None,
                   db_conn: Any = None,
                   committable: bool = None,
                   errors: list[str] = None) -> Sob | None:
        """
        Retrieve the single instance of the *SOB* object from the database, as per the criteria provided.

        Optionally, *joins* holds a list of tuples specifying the table joins, with the following format:
            1. the first element in the tuple identifies the table:
                - a singlet, with the type of the *Sob* subclass whose database table is to be joined
            2. a 2/3/4-tuple, or a list of 2/3/4-tuples, informs on the *ON* fragment conditions:
                - the first attribute, as a string or a *StrEnum* instance
                - the second attribute, as a string or a *StrEnum* instance
                - the operation ("=", "<>", "<=", or ">=", defaults to "=")
                - the connector to the next condition ("and" or "or", defaults to "and")
            3. the third element is the type of the join ("inner", "full", "left", "right", defaults to "inner")

        Optionally, selection criteria may be specified in *where_clause* and *where_vals*, or additionally but
        preferably, by key-value pairs in *where_data*. Care should be exercised if *where_clause* contains *IN*
        directives. In PostgreSQL, the list of values for an attribute with the *IN* directive must be contained
        in a specific tuple, and the operation will break for a list of values containing only 1 element.
        The safe way to specify *IN* directives is to add them to *where_data*, as the specifics of each DB flavor
        will then be properly dealt with.

        The syntax specific to *where_data*'s key/value pairs is as follows:
            1. *key*:
                - an attribute (*StrEnum*, or *str* possibly aliased), or
                - a 2/3-tuple with an attribute and the corresponding SQL comparison operation
                  ("=", ">", "<", ">=", "<=", "<>", "in", "like", "between" - defaults to "="), followed
                  by a SQL logical operator relating it to the next item ("and", "or" - defaults to "and")
            2. *value*:
                - a scalar, or a list, or an expression possibly containing other attribute(s)

        If more than 1 tuple in the database satisfy the selection criteria, an error is flagged. If the query
        yields no tuples, no errors are flagged and *None* is returned.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param __references: the *SOB* references to load at object instantiation time
        :param joins: optional *JOIN* clauses to use
        :param where_clause: optional criteria for tuple selection
        :param where_vals: values to be associated with the selection criteria
        :param where_data: the selection criteria specified as key-value pairs
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: the instantiated *SOB* object, or *None* if not found or error
        """
        # inicialize the return variable
        result: Sob | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        if cls:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            logger: Logger = sob_loggers.get(cls_name)

            # obtain the aliases map
            aliases: dict[str, str] = PySob.__build_aliases_map(subcls=cls,
                                                                joins=joins)
            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(subcls=cls,
                                                         aliases=aliases,
                                                         joins=joins)
            # build the attributes list
            alias: str = aliases.get(cls.__qualname__)
            attrs: list[str] = [f"{alias}.{attr}" for attr in sob_db_columns.get(cls_name)]

            # retrieve the data
            where_data = PySob.__add_aliases(attrs=where_data,
                                             aliases=aliases)
            sel_stmt: str = f"SELECT {', '.join(attrs)} FROM {from_clause}"
            recs: list[tuple[int | str]] = db_select(sel_stmt=sel_stmt,
                                                     where_clause=where_clause,
                                                     where_vals=where_vals,
                                                     where_data=where_data,
                                                     min_count=0,
                                                     max_count=1,
                                                     engine=db_engine,
                                                     connection=db_conn,
                                                     committable=committable,
                                                     errors=errors,
                                                     logger=logger)
            if recs:
                # build the SOB object
                sob: Sob = cls()
                data: dict[str, Any] = {}
                for inx, attr in enumerate(sob_db_columns.get(cls_name)):
                    data[attr] = recs[0][inx]
                    sob.set(data=data)

                if not __references or PySob.__load_references(__references,
                                                               objs=[sob],
                                                               db_engine=db_engine,
                                                               db_conn=db_conn,
                                                               committable=committable,
                                                               errors=errors) is not None:
                    result = sob

        return result

    # noinspection PyPep8
    @staticmethod
    def retrieve(__references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
                 /,
                 joins: list[tuple[type[Sob],
                                   list[tuple[StrEnum, StrEnum,
                                              Literal["=", "<>", "<=", ">="] | None,
                                              Literal["and", "or"] | None]],
                                   Literal["inner", "full", "left", "right"] | None]] | list[tuple] = None,
                 where_clause: str = None,
                 where_vals: tuple = None,
                 where_data: dict[StrEnum | tuple |
                                  tuple[StrEnum,
                                        Literal["=", ">", "<", ">=", "<=",
                                                "<>", "in", "like", "between"] | None,
                                        Literal["and", "or"] | None], Any] = None,
                 orderby_clause: StrEnum |
                                 tuple[StrEnum, Literal["asc", "desc"] | None] |
                                 list[StrEnum | tuple[StrEnum, Literal["asc", "desc"] | None]] = None,
                 min_count: int = None,
                 max_count: int = None,
                 offset_count: int = None,
                 limit_count: int = None,
                 db_engine: DbEngine = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> list[Sob] | None:
        """
        Retrieve the instances of *SOB* objects from the database, as per the criteria provided.

        Optionally, *joins* holds a list of tuples specifying the table joins, with the following format:
            1. the first element in the tuple identifies the table:
                - a singlet, with the type of the *Sob* subclass whose database table is to be joined
            2. a 2/3/4-tuple, or a list of 2/3/4-tuples, informs on the *ON* fragment conditions:
                - the first attribute, as a string or a *StrEnum* instance
                - the second attribute, as a string or a *StrEnum* instance
                - the operation ("=", "<>", "<=", or ">=", defaults to "=")
                - the connector to the next condition ("and" or "or", defaults to "and")
            3. the third element is the type of the join ("inner", "full", "left", "right", defaults to "inner")

        Selection criteria may be specified in *where_clause* and *where_vals*, or additionally but preferably,
        by key-value pairs in *where_data*. Care should be exercised if *where_clause* contains *IN* directives.
        In PostgreSQL, the list of values for an attribute with the *IN* directive must be contained in a
        specific tuple, and the operation will break for a list of values containing only 1 element.
        The safe way to specify *IN* directives is to add them to *where_data*, as the specifics of each DB
        flavor will then be properly dealt with.

        The syntax specific to *where_data*'s key/value pairs is as follows:
            1. *key*:
                - an attribute (*StrEnum*, or *str* possibly aliased), or
                - a 2/3-tuple with an attribute and the corresponding SQL comparison operation
                  ("=", ">", "<", ">=", "<=", "<>", "in", "like", "between" - defaults to "="), followed
                  by a SQL logical operator relating it to the next item ("and", "or" - defaults to "and")
            2. *value*:
                - a scalar, or a list, or an expression possibly containing other attribute(s)

        The sort order of the query results may be specified by *orderby_clause*, which might be:
            - a *str* or *StrEnum* indicating the attribute and the default sort direction *asc*, or
            - a tuple or a list of tuples, each indicating an attribute and its sort direction (defaults to *asc*)

        If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
        If both *min_count* and *max_count* are specified with equal values, then exactly that number of
        tuples must be returned by the query. The parameter *offset_count* is used to offset the retrieval
        of tuples. For both *offset_count* and *limit_count* to be used together with SQLServer, an *ORDER BY*
        clause must have been specifed, otherwise a runtime error is raised.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param __references: the *SOB* references to load at object instantiation time
        :param joins: optional *JOIN* clauses to use
        :param where_clause: optional criteria for tuple selection
        :param where_vals: values to be associated with the selection criteria
        :param where_data: the selection criteria specified as key-value pairs
        :param orderby_clause: optional retrieval order
        :param min_count: optionally defines the minimum number of tuples expected to be retrieved
        :param max_count: optionally defines the maximum number of tuples expected to be retrieved
        :param offset_count: number of tuples to skip (defaults to none)
        :param limit_count: limit to the number of tuples returned, to be specified in the query statement itself
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: a list with the objects instantiated, *[]* on empty result, or *None* if error
        """
        # inicialize the return variable
        result: list[Sob] | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        if cls:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            logger: Logger = sob_loggers.get(cls_name)

            # obtain the aliases map
            aliases: dict[str, str] = PySob.__build_aliases_map(subcls=cls,
                                                                joins=joins)
            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(subcls=cls,
                                                         aliases=aliases,
                                                         joins=joins)
            # build the attributes list
            alias: str = aliases.get(cls.__qualname__)
            attrs: list[str] = [f"{alias}.{attr}" for attr in sob_db_columns.get(cls_name)]

            # normalize the 'ORDER BY' clause
            if orderby_clause:
                orderby_clause = PySob.__normalize_orderby(orderby=orderby_clause,
                                                           aliases=aliases)
            # retrieve the data
            where_data = PySob.__add_aliases(attrs=where_data,
                                             aliases=aliases)
            sel_stmt: str = f"SELECT DISTINCT {', '.join(attrs)} FROM {from_clause}"
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
            if recs is not None:
                # build the objects list
                objs: list[Sob] = []
                for rec in recs:
                    data: dict[str, Any] = {}
                    for inx, attr in enumerate(sob_db_columns.get(cls_name)):
                        data[attr] = rec[inx]
                    sob: type[Sob] = cls()
                    sob.set(data=data)
                    objs.append(sob)

                if not __references or not objs or PySob.__load_references(__references,
                                                                           objs=objs,
                                                                           db_engine=db_engine,
                                                                           db_conn=db_conn,
                                                                           committable=committable,
                                                                           errors=errors) is not None:
                    result = objs

        return result

    @staticmethod
    def erase(where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[StrEnum | tuple |
                               tuple[StrEnum,
                                     Literal["=", ">", "<", ">=", "<=",
                                             "<>", "in", "like", "between"] | None,
                                     Literal["and", "or"] | None], Any] = None,
              min_count: int = None,
              max_count: int = None,
              db_engine: DbEngine = None,
              db_conn: Any = None,
              committable: bool = None,
              errors: list[str] = None) -> int | None:
        """
        Erase touples from the corresponding database table, as per the criteria provided.

        Optionally, selection criteria may be specified in *where_clause* and *where_vals*, or additionally but
        preferably, by key-value pairs in *where_data*. Care should be exercised if *where_clause* contains *IN*
        directives. In PostgreSQL, the list of values for an attribute with the *IN* directive must be contained
        in a specific tuple, and the operation will break for a list of values containing only 1 element.
        The safe way to specify *IN* directives is to add them to *where_data*, as the specifics of each DB flavor
        will then be properly dealt with.

        The syntax specific to *where_data*'s key/value pairs is as follows:
            1. *key*:
                - an attribute (*StrEnum*, or *str* possibly aliased), or
                - a 2/3-tuple with an attribute and the corresponding SQL comparison operation
                  ("=", ">", "<", ">=", "<=", "<>", "in", "like", "between" - defaults to "="), followed
                  by a SQL logical operator relating it to the next item ("and", "or" - defaults to "and")
            2. *value*:
                - a scalar, or a list, or an expression possibly containing other attribute(s)

        If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
        If both *min_count* and *max_count* are specified with equal values, then exactly that number of
        tuples must be deleted from the database table.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param where_clause: optional criteria for tuple selection
        :param where_vals: values to be associated with the selection criteria
        :param where_data: the selection criteria specified as key-value pairs
        :param min_count: optionally defines the minimum number of tuples expected to be retrieved
        :param max_count: optionally defines the maximum number of tuples expected to be retrievedement itself
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: the number of deleted tuples, or *None* if error
        """
        # initialize the return variable
        result: int | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        if cls:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            logger: Logger = sob_loggers.get(cls_name)
            tbl_name: str = sob_db_specs[cls_name][0]

            # delete specified rows
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
        return result

    @staticmethod
    def store(insert_data: dict[str, Any] = None,
              return_cols: dict[str, Any] = None,
              db_engine: DbEngine = None,
              db_conn: Any = None,
              committable: bool = None,
              errors: list[str] = None) -> tuple | int | None:
        """
        Persist the data in *insert_data* in the corresponding database table, with an *INSERT* operation.

        The optional *return_cols* indicate that the values of the columns therein should be returned.
        This is useful to retrieve values from identity columns (that is, columns whose values at insert time
        are handled by the database).

        The target database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *connection* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param insert_data: data to be inserted as key-value pairs
        :param return_cols: optional columns and respective types, whose values are to be returned
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: the values of *return_cols*, the number of inserted tuples (0 ou 1), or *None* if error
        """
        # initialize the return variable
        result: tuple | int | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        # delete specified rows
        if cls:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"
            logger: Logger = sob_loggers.get(cls_name)
            tbl_name: str = sob_db_specs[cls_name][0]

            # persis the data
            result = db_insert(insert_stmt=f"INSERT INTO {tbl_name}",
                               insert_data=insert_data,
                               return_cols=return_cols,
                               engine=db_engine,
                               connection=db_conn,
                               committable=committable,
                               errors=errors,
                               logger=logger)
        return result

    # noinspection PyPep8
    @staticmethod
    def add_aliases(attrs: list[str | StrEnum] |
                           dict[str | StrEnum, Any] | None,
                    classes: list[type[Sob]] = None,
                    errors: list[str] = None) -> list[str] | dict[str, Any] | None:
        """
        Add the appropriate database aliases to the names of the database attributes in *attrs*.

        An alias is obtained by concatenating the uppercase letters of the *Sob* subclass name and setting
        it to lowercase. If a conflict arises, subsequent lowercase letters from the *Sob* subclass name
        are added to the alias, until the alias becomes unique. If specified, *classes* will be considered first,
        immediately after the invoking class itself.

        :param attrs: the *list* or *dict* containing the database attributes
        :param classes: optional list of pre-defined *Sob* subclasses
        :param errors: incidental error messages (might be a non-empty list)
        :return: the *list* or *dict* containing the aliased database attributes
        """
        # initialize the return variable
        result: list[str] | dict[str, Any] | None = None

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        if cls:
            # instantiate the aliases map
            aliases: dict[str, str] = {}

            # register the invoking class first
            PySob.__register_class_alias(subcls=cls,
                                         aliases=aliases)

            # then the list of *Sob* subclasses
            if classes:
                for subcls in classes:
                    PySob.__register_class_alias(subcls=subcls,
                                                 aliases=aliases)
            # then the attributes
            for attr in attrs:
                if isinstance(attr, StrEnum):
                    subcls = attr.__class__
                    PySob.__register_class_alias(subcls=subcls,
                                                 aliases=aliases)
            # add the aliases
            result = PySob.__add_aliases(attrs=attrs,
                                         aliases=aliases)
        return result

    # noinspection PyPep8
    @staticmethod
    def build_from_clause(joins: list[tuple[type[Sob],
                                            list[tuple[StrEnum, StrEnum,
                                                       Literal["=", "<>", "<=", ">="] | None,
                                                       Literal["and", "or"] | None]],
                                            Literal["inner", "full", "left", "right"] | None]] | list[tuple],
                          classes: list[type[Sob]] = None,
                          errors: list[str] = None) -> str:
        """
        Build the query's *FROM* clause.

        The parameter *joins* holds a list of tuples specifying the table joins, with the following format:
            1. the first element in the tuple identifies the table:
                - a singlet, with the type of the *Sob* subclass whose database table is to be joined
            2. a 2/3/4-tuple, or a list of 2/3/4-tuples, informs on the *ON* fragment conditions:
                - the first attribute, as a string or a *StrEnum* instance
                - the second attribute, as a string or a *StrEnum* instance
                - the operation ("=", "<>", "<=", or ">=", defaults to "=")
                - the connector to the next condition ("and" or "or", defaults to "and")
            3. the third element is the type of the join ("inner", "full", "left", "right", defaults to "inner")

        :param joins: the list of *JOIN* clauses
        :param classes: optional list of *SOB* subclasses to assist with alias determination
        :param errors: incidental error messages (might be a non-empty list)
        :return: the *FROM* clause containing the *JOIN*s
        """
        # initialize the return variable
        result: str | None = None

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors)

        if cls:
            # obtain the aliases map
            aliases: dict[str, str] = PySob.__build_aliases_map(subcls=cls,
                                                                joins=joins,
                                                                classes=classes)
            # build the 'FROM' clause
            result = PySob.__build_from_clause(subcls=cls,
                                               aliases=aliases,
                                               joins=joins)
        return result

    @staticmethod
    def __add_alias(attr: str | StrEnum,
                    aliases: dict[str, str]) -> str:
        """
        Add the appropriate database alias to *attr*.

        :param attr: the database attribute
        :param aliases: mapping of *Sob* subclasses to aliases
        :return: the aliased attribute
        """
        if isinstance(attr, StrEnum):
            subcls_name: str = attr.__class__.__qualname__.rsplit(".", 1)[0]
            attr_alias: str = aliases.get(subcls_name, "")
            if attr_alias:
                attr_alias = attr_alias + "."
            result: str = attr_alias + attr.value.lower()
        else:
            result: str = attr

        return result

    @staticmethod
    def __add_aliases(attrs: list[str | StrEnum] | dict[str | StrEnum, Any],
                      aliases: dict[str, str]) -> list[str] | dict[str, Any]:
        """
        Add the appropriate database aliases to the names of the database attributes in *attrs*.

        :param attrs: the *list* or *dict* containing the database attributes
        :param aliases: mapping of *Sob* subclasses to aliases
        :return: the *list* or *dict* containing the aliased database attributes
        """
        # initialize the return variable
        result: list[str] | dict[str, Any] | None = None
        if isinstance(attrs, list):
            result: list[str] = []
            for attr in attrs:
                aliased_attr: str = PySob.__add_alias(attr=attr,
                                                      aliases=aliases)
                result.append(aliased_attr)
        elif isinstance(attrs, dict):
            result: dict[str, str] = {}
            for attr, val in attrs.items():
                # handle 'where_data' with complex keys
                if isinstance(attr, list | tuple):
                    aliased_attr: str = PySob.__add_alias(attr=attr[0],
                                                          aliases=aliases)
                    if isinstance(attr, list):
                        attr = [aliased_attr, *attr[1:]]
                    else:
                        attr = (aliased_attr, *attr[1:])
                    result[attr] = val
                else:
                    aliased_attr: str = PySob.__add_alias(attr=attr,
                                                          aliases=aliases)
                    result[aliased_attr] = val

        return result

    # noinspection PyPep8
    @staticmethod
    def __build_aliases_map(subcls: type[Sob],
                            joins: list[tuple[type[Sob],
                                              list[tuple[StrEnum, StrEnum,
                                                         Literal["=", "<>", "<=", ">="] | None,
                                                         Literal["and", "or"] | None]],
                                              Literal["inner", "full", "left", "right"] | None]] | list[tuple],
                            classes: list[type[Sob]] = None) -> dict[str, str]:
        """
        Map the *Sob* subclasses to database aliases.

        The parameter *joins* holds a list of tuples specifying the table joins, with the following format:
            1. the first element in the tuple identifies the table:
                - a singlet, with the type of the *Sob* subclass whose database table is to be joined
            2. a 2/3/4-tuple, or a list of 2/3/4-tuples, informs on the *ON* fragment conditions:
                - the first attribute, as a string or a *StrEnum* instance
                - the second attribute, as a string or a *StrEnum* instance
                - the operation ("=", "<>", "<=", or ">=", defaults to "=")
                - the connector to the next condition ("and" or "or", defaults to "and")
            3. the third element is the type of the join ("inner", "full", "left", "right", defaults to "inner")

        :param subcls: the reference *Sob* subclass
        :param joins: the list of *JOIN* clauses
        :param classes: optional list of *Sob* subclasses to assist with alias determination
        :return: the mapping of aliases to *Sob* subclasses
        """
        # initialize the return variable
        result: dict[str, str] = {}

        # register the alias for the master subclass
        PySob.__register_class_alias(subcls=subcls,
                                     aliases=result)
        # register the subclasses
        for cls in classes or []:
            PySob.__register_class_alias(subcls=cls,
                                         aliases=result)

        # traverse the joins (only the first element in each tuple is relevant)
        for join in joins or []:
            join_cls: type[Sob] = join[0]
            PySob.__register_class_alias(subcls=join_cls,
                                         aliases=result)
        return result

    # noinspection PyPep8
    @staticmethod
    def __build_from_clause(subcls: type[Sob],
                            aliases: dict[str, str],
                            joins: list[tuple[type[Sob],
                                              list[tuple[StrEnum, StrEnum,
                                                         Literal["=", "<>", "<=", ">="] | None,
                                                         Literal["and", "or"] | None]],
                                              Literal["inner", "full", "left", "right"] | None]] |
                                   list[tuple] = None) -> str:
        """
        Build the query's *FROM* clause.

        Optionally, *joins* holds a list of tuples specifying the table joins, with the following format:
            1. the first element in the tuple identifies the table:
                - a singlet, with the type of the *Sob* subclass whose database table is to be joined
            2. a 2/3/4-tuple, or a list of 2/3/4-tuples, informs on the *ON* fragment conditions:
                - the first attribute, as a string or a *StrEnum* instance
                - the second attribute, as a string or a *StrEnum* instance
                - the operation ("=", "<>", "<=", or ">=", defaults to "=")
                - the connector to the next condition ("and" or "or", defaults to "and")
            3. the third element is the type of the join ("inner", "full", "left", "right", defaults to "inner")

        :param subcls: the reference *Sob* subclass
        :param aliases: mapping of *Sob* subclasses to aliases
        :param joins: the list of *JOIN* clauses
        :return: the *FROM* clause containing the *JOIN*s
        """
        # obtain the the fully-qualified name of the class type
        cls_name: str = f"{subcls.__module__}.{subcls.__qualname__}"

        # initialize the return variable
        result: str = sob_db_specs[cls_name][0] + " AS " + aliases.get(subcls.__qualname__)

        # traverse the joins
        for join in joins or []:
            join_cls: str = f"{join[0].__module__}.{join[0].__qualname__}"
            table_name: str = sob_db_specs[join_cls][0]
            join_alias: str = aliases.get(join_cls[join_cls.rindex(".")+1:])
            join_mode: str = join[3].upper() if len(join) > 3 else "INNER"
            result += f" {join_mode} JOIN {table_name} AS {join_alias} ON "

            # traverse the mandatory 'ON' specs
            on_specs: list[tuple] = join[1] if isinstance(join[1], list) else [join[1]]
            for on_spec in on_specs:
                op: str = on_spec[2] if len(on_spec) > 2 and on_spec[2] in ["=", "<>", "<=", ">="] else "="
                result += (PySob.__add_alias(attr=on_spec[0],
                                             aliases=aliases) + " " + op + " " +
                           PySob.__add_alias(attr=on_spec[1],
                                             aliases=aliases))
                con: str = on_spec[-1].upper() if  len(on_spec) > 2 and on_spec[-1] in ["and", "or"] else "AND"
                result += " " + con + " "
            result = result[:-5] if result.endswith(" AND ") else result[:-4]

        return result

    # noinspection PyPep8
    @staticmethod
    def __normalize_orderby(orderby: StrEnum |
                                     tuple[StrEnum, Literal["asc", "desc"] | None] |
                                     list[StrEnum | tuple[StrEnum, Literal["asc", "desc"] | None]],
                            aliases: dict[str, str]) -> str:
        """
        Normalize the *ORDER BY* query clause, by adding aliases and converting to a *str*.

        The sort order of the query results as might be specified by *orderby_clause*:
            - a *str* or *StrEnum* indicating the attribute and the default sort direction *asc*, or
            - a tuple or a list of tuples, each indicating an attribute and its sort direction (defaults to *asc*)

        :param orderby: the query retrieval order
        :return: the normalized *ORDER BY* clause as *str*, with added aliases
        """
        # initialize the return variable
        result: str = ""

        # make sure 'orderby' is a list
        orderby = orderby if isinstance(orderby, list) else [orderby]

        # traverse the parameter elements
        for item in orderby:
            attr: str | StrEnum
            sort_dir: str
            # noinspection PyUnreachableCode
            if isinstance(item, tuple):
                attr = item[0]
                sort_dir = item[1].upper() if len(item) > 1 and item[1] in ["asc", "desc"] else "ASC"
            else:
                attr = item
                sort_dir = "ASC"
            result += PySob.__add_alias(attr=attr,
                                        aliases=aliases) + " " + sort_dir + ", "

        return result[:-2]

    @staticmethod
    def __get_invoking_class(errors: list[str] = None,
                             logger: Logger = None) -> type[Sob] | None:
        """
        Retrieve the fully-qualified type of the subclass currently being accessed.

        :param errors: incidental error messages (might be a non-empty list)
        :param logger: optional logger
        :return: the fully-qualified type of the subclass currently being accessed, or *None* if error
        :
        """
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
                        logger.warning(msg=exc_format(exc=e,
                                                      exc_info=sys.exc_info()))
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
                    logger.warning(msg=exc_format(exc=e,
                                                  exc_info=sys.exc_info()))

        if not result:
            msg: str = (f"Unable to obtain class '{classname}', "
                        f"filepath '{filepath}', from invoking function '{invoking_function}'")
            if logger:
                logger.error(msg=f"{msg} - invocation frame {caller_frame}")
            if isinstance(errors, list):
                errors.append(msg)

        return result

    @staticmethod
    def __register_class_alias(subcls: type[Sob],
                               aliases: dict[str, str]) -> None:
        """
        Register the alias for the *Sob* subclass *subcls*.

        An alias is obtained by concatenating the uppercase letters of the *Sob* subclass name and setting
        it to lowercase. If a conflict arises, subsequent lowercase letters from the *Sob* subclass name
        are added to the alias, until the alias becomes unique.

        :param subcls: the reference *Sob* subclass
        :param aliases: the aliases registry
        """
        subcls_name: str = subcls.__qualname__.rsplit(".", 1)[0]
        if subcls_name not in aliases:
            cls_alias: str = "".join([c for c in subcls_name if c.isupper()]).lower()
            pos: int = 0
            while dict_has_value(source=aliases,
                                 value=cls_alias):
                remainder: str = subcls_name[pos:]
                for c in remainder:
                    pos += 1
                    if c.islower():
                        cls_alias += c
                        break
            aliases[subcls_name] = cls_alias

    @staticmethod
    def __load_references(__references:  type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                          /,
                          objs: list[Sob],
                          db_engine: DbEngine | None,
                          db_conn: Any | None,
                          committable: bool | None,
                          errors: list[str]) -> bool | None:
        """
        Load the *SOB* references specified in *__references* from the database.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param __references: the *SOB* references to load
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages (might be a non-empty list)
        :return: *True* if at least one references was load, *False* otherwise, or *None* if error
        """
        # make sure '__references' is a list
        if not isinstance(__references, list):
            __references = [__references]

        # initialize the return variable
        result: bool | None = len(objs) > 0 and len(__references) > 0

        # iniialize a local errors list
        curr_errors: list[str] = []

        if SOB_MAX_THREADS > 1 and (len(objs) > 1 or len(__references) > 1):
            task_futures: list[Future] = []
            with ThreadPoolExecutor(max_workers=SOB_MAX_THREADS) as executor:
                for obj in objs:
                    for reference in __references if isinstance(__references, list) else [__references]:
                        # must not multiplex 'db_conn'
                        future: Future = executor.submit(obj.load_references,
                                                         reference,
                                                         db_engine=db_engine,
                                                         errors=curr_errors)
                        if curr_errors:
                            break
                        task_futures.append(future)
                    if curr_errors:
                        break

            # wait for all task futures to complete, then shutdown down the executor
            futures.wait(fs=task_futures)
            executor.shutdown(wait=False)
        else:
            for obj in objs:
                obj.load_references(__references,
                                    db_engine=db_engine,
                                    db_conn=db_conn,
                                    committable=committable,
                                    errors=curr_errors)
                if curr_errors:
                    break

        if curr_errors:
            result = None
            if isinstance(errors, list):
                errors.extend(curr_errors)

        return result
