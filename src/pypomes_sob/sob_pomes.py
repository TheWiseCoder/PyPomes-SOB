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
from typing import Any, Literal, TypeVar

from .sob_config import (
    SOB_BASE_FOLDER, SOB_MAX_THREADS,
    sob_db_columns, sob_db_specs, sob_attrs_input, sob_attrs_unique
)

# 'Sob' stands for all subclasses of 'PySob'
Sob = TypeVar("Sob",
              bound="PySob")


class PySob:
    """
    Root entity for the *SOB* (Simple object) hierarchy.

    The *SOB* objects are mapped to a *RDBMS* table, and present relationships within and among themselves
    typical of the relational paradigm, such as primary and foreign keys, nullability, uniqueness, one-to-many,
    many-to-many, to mention just a few.
    """

    def __init__(self,
                 __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
                 /,
                 where_data: dict[str, Any] = None,
                 db_engine: DbEngine = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None,
                 logger: Logger = None) -> None:
        """
        Instantiate a *SOB* object from its subclass.

        If loading the corresponding data from a database is desired, *where_data* should be specifed,
        and its contents should yield just one tuple when used in a *SELECT* statement.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        If provided, *logger* is used, and saved for further usage in operations involving the object.

        :param __references: the *SOB* references to load at object instantiation time
        :param where_data: the criteria to load the oject's data from the database
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages
        :param logger: optional logger
        """
        self._logger: Logger = logger
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
        :param errors: incidental error messages
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
        """
        Attempt to persist the current state of the object in the database with an *UPDATE* operation.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages
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
        :param errors: incidental error messages
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
        :param errors: incidental error messages
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
        tbl_name = sob_db_specs[cls_name][0]
        for key, value in data.items():
            if isinstance(value, Enum) and "use_names" in value.__class__:
                # use enum names assigned as values in 'data'
                value = value.name
            if key in self.__dict__:
                self.__dict__[key] = value
            elif self._logger:
                self._logger.warning(msg=f"'{key}' is not an attribute of '{tbl_name}'")

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
        :param errors: incidental error messages
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
            result = db_exists(table=tbl_name,
                               where_data=where_data,
                               engine=db_engine,
                               connection=db_conn,
                               committable=committable,
                               errors=errors,
                               logger=self._logger)
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
        :param errors: incidental error messages
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
        attrs: list[str] = self.get_columns()
        recs: list[tuple] = db_select(sel_stmt=f"SELECT {', '.join(attrs)} FROM {tbl_name}",
                                      where_data=where_data,
                                      limit_count=2,
                                      engine=db_engine,
                                      connection=db_conn,
                                      committable=committable,
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
            pk_name: str = sob_db_columns[cls_name][0]
            rec: tuple = recs[0]
            for inx, attr in enumerate(attrs):
                # PK attribute in DB table might have a different name
                if attr == pk_name:
                    self.__dict__["id"] = rec[inx]
                else:
                    self.__dict__[attr] = rec[inx]
            if __references:
                PySob.__load_references(__references,
                                        objs=[self],
                                        db_engine=db_engine,
                                        db_conn=db_conn,
                                        committable=committable,
                                        errors=errors)
            result = not errors

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
        elif sob_attrs_unique[cls_name]:
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

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param __references: the *SOB* references to load
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages
        """
        # must be implemented by subclasses containing references
        msg: str = (f"Subclass {self.__class__.__module__}.{self.__class__.__qualname__} "
                    "failed to implement 'load_references()'")
        if isinstance(errors, list):
            errors.append(msg)
        if self._logger:
            self._logger.error(msg=msg)

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

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        Whenever needed, this operation must be by the subclass level.

        :param __references: the *SOB* references to invalidate
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages
        """
        # must be implemented by subclasses containing references
        msg: str = (f"Subclass {self.__class__.__module__}.{self.__class__.__qualname__} "
                    "failed to implement 'invalidate_references()'")
        if isinstance(errors, list):
            errors.append(msg)
        if self._logger:
            self._logger.error(msg=msg)

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

        :param db_specs: the attributes mapped to the corresponding database table
        :param attrs_unique: the sets of attributes defining object's uniqueness in the database
        :param attrs_input: mapping of names used in input to actual attribute names
        :param logger: optional logger
        """
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
            sob_db_columns.update({cls_name: tuple(attrs)})

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

    # noinspection PyPep8
    @staticmethod
    def count(alias: str = None,
              joins: list[tuple[type[Sob] | str, str, str]] |
                     list[tuple[type[Sob] | str, str, str, Literal["inner", "full", "left", "right"]]] = None,
              count_clause: str = None,
              where_clause: str = None,
              where_vals: tuple = None,
              where_data: dict[str, Any] = None,
              db_engine: DbEngine = None,
              db_conn: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> int | None:
        """
        Count the occurrences of tuples in the corresponding database table, as per the criteria provided.

        Optionally, selection criteria may be specified in *where_clause*, or additionally by
        key-value pairs in *where_data*, which would be concatenated by the *AND* logical connector.
        Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
        for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
        break for a list of values containing only 1 element. The safe way to specify *IN* directives is
        to add them to *where_data*, as the specifics of each DB flavor will then be properly dealt with.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param alias: optional alias for the database table in the *SELECT* statement
        :param joins: optional *JOIN* clauses to use
        :param count_clause: optional parameters in the *COUNT* clause (defaults to 'COUNT(*)')
        :param where_clause: optional criteria for tuple selection
        :param where_vals: values to be associated with the selection criteria
        :param where_data: the selection criteria specified as key-value pairs
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages
        :param logger: optional logger
        :return: the number of tuples counted, or *None* if error
        """
        # inicialize the return variable
        result: int | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        if not errors:
            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(cls=cls,
                                                         alias=alias,
                                                         joins=joins)
            # retrieve the data
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
        if errors and logger:
            logger.error(msg="; ".join(errors))

        return result

    # noinspection PyPep8
    @staticmethod
    def exists(alias: str = None,
               joins: list[tuple[type[Sob] | str, str, str]] |
                      list[tuple[type[Sob] | str, str, str, Literal["inner", "full", "left", "right"]]] = None,
               where_clause: str = None,
               where_vals: tuple = None,
               where_data: dict[str, Any] = None,
               min_count: int = None,
               max_count: int = None,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None,
               logger: Logger = None) -> bool | None:
        """
        Determine if at least one tuple exists in the database table, as per the criteria provided.

        Optionally, selection criteria may be specified in *where_clause*, or additionally by
        key-value pairs in *where_data*, which would be concatenated by the *AND* logical connector.
        Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
        for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
        break for a list of values containing only 1 element. The safe way to specify *IN* directives is
        to add them to *where_data*, as the specifics of each DB flavor will then be properly dealt with.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param alias: optional alias for the database table in the *SELECT* statement
        :param joins: optional *JOIN* clauses to use
        :param where_clause: optional criteria for tuple selection
        :param where_vals: values to be associated with the selection criteria
        :param where_data: the selection criteria specified as key-value pairs
        :param min_count: optionally defines the minimum number of tuples expected to exist
        :param max_count: optionally defines the maximum number of tuples expected to exist
        :param db_engine: the reference database engine (uses the default engine, if not provided)
        :param db_conn: optional connection to use (obtains a new one, if not provided)
        :param committable: whether to commit the database operations upon errorless completion
        :param errors: incidental error messages
        :param logger: optional logger
        :return: *True* if the criteria for tuple existence were met, *False* otherwise, or *None* if error
        """
        # inicialize the return variable
        result: bool | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        if not errors:
            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(cls=cls,
                                                         alias=alias,
                                                         joins=joins)
            # execute the query
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
        if errors and logger:
            logger.error(msg="; ".join(errors))

        return result

    # noinspection PyPep8
    @staticmethod
    def get_values(attrs: tuple[str],
                   alias: str = None,
                   joins: list[tuple[type[Sob] | str, str, str]] |
                          list[tuple[type[Sob] | str, str, str, Literal["inner", "full", "left", "right"]]] = None,
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
                   logger: Logger = None) -> list[tuple] | None:
        """
        Retrieve the values of *attrs* from the database, as per the criteria provided.

        Optionally, selection criteria may be specified in *where_clause*, or additionally by
        key-value pairs in *where_data*, which would be concatenated by the *AND* logical connector.
        Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
        for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
        break for a list of values containing only 1 element. The safe way to specify *IN* directives is
        to add them to *where_data*, as the specifics of each DB flavor will then be properly dealt with.

        If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
        If both *min_count* and *max_count* are specified with equal values, then exactly that number of
        tuples must be returned by the query. The parameter *offset_count* is used to offset the retrieval
        of tuples. For both *offset_count* and *limit_count* to be used together with SQLServer, an *ORDER BY*
        clause must have been specifed, otherwise a runtime error is raised.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param attrs: one or more attributes whose values to retrieve
        :param alias: optional alias for the database table in the *SELECT* statement
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
        :param errors: incidental error messages
        :param logger: optional logger
        :return: a list containing tuples with the values retrieved, *[]* on empty result, or *None* if error
        """
        # inicialize the return variable
        result: list[tuple] | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        if not errors:
            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(cls=cls,
                                                         alias=alias,
                                                         joins=joins)
            # retrieve the data
            result = db_select(sel_stmt=f"SELECT DISTINCT {', '.join(attrs)} FROM {from_clause}",
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

    # noinspection PyPep8
    @staticmethod
    def retrieve(__references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
                 /,
                 alias: str = None,
                 joins: list[tuple[type[Sob] | str, str, str]] |
                        list[tuple[type[Sob] | str, str, str, Literal["inner", "full", "left", "right"]]] = None,
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
        """
        Retrieve the instances of *SOB* objects from the database, as per the criteria provided.

        Optionally, selection criteria may be specified in *where_clause*, or additionally by
        key-value pairs in *where_data*, which would be concatenated by the *AND* logical connector.
        Care should be exercised if *where_clause* contains *IN* directives. In PostgreSQL, the list of values
        for an attribute with the *IN* directive must be contained in a specific tuple, and the operation will
        break for a list of values containing only 1 element. The safe way to specify *IN* directives is
        to add them to *where_data*, as the specifics of each DB flavor will then be properly dealt with.

        If not positive integers, *min_count*, *max_count*, *offset_count*, and *limit_count* are ignored.
        If both *min_count* and *max_count* are specified with equal values, then exactly that number of
        tuples must be returned by the query. The parameter *offset_count* is used to offset the retrieval
        of tuples. For both *offset_count* and *limit_count* to be used together with SQLServer, an *ORDER BY*
        clause must have been specifed, otherwise a runtime error is raised.

        The targer database engine, specified or default, must have been previously configured.
        The parameter *committable* is relevant only if *db_conn* is provided, and is otherwise ignored.
        A rollback is always attempted, if an error occurs.

        :param __references: the *SOB* references to load at object instantiation time
        :param alias: optional alias for the database table in the *SELECT* statement
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
        :param errors: incidental error messages
        :param logger: optional logger
        :return: a list with the objects instantiated, *[]* on empty result, or *None* if error
        """
        # inicialize the return variable
        result: list[Sob] | None = None

        # make sure to have an errors list
        if not isinstance(errors, list):
            errors = []

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class(errors=errors,
                                                    logger=logger)
        if not errors:
            cls_name: str = f"{cls.__module__}.{cls.__qualname__}"

            # build the FROM clause
            from_clause: str = PySob.__build_from_clause(cls=cls,
                                                         alias=alias,
                                                         joins=joins)
            # build the attributes list
            attrs: list[str] = []
            for attr in sob_db_columns.get(cls_name):
                if alias:
                    attr = f"{alias}.{attr}"
                attrs.append(attr)

            # retrieve the data
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
            if not errors:
                # build the objects list
                objs: list[Sob] = []
                for rec in recs:
                    data: dict[str, Any] = {}
                    for inx, attr in enumerate(sob_db_columns.get(cls_name)):
                        data[attr] = rec[inx]
                    sob: type[Sob] = cls()
                    sob.set(data=data)
                    if errors:
                        break
                    objs.append(sob)

                if __references:
                    PySob.__load_references(__references,
                                            objs=objs,
                                            db_engine=db_engine,
                                            db_conn=db_conn,
                                            committable=committable,
                                            errors=errors)
                if not errors:
                    result = objs

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
        """
        Erase touples from the corresponding database table, as per the criteria provided.

        The values for selecting the tuples to be deleted are in *where_vals*, and/or additionally specified
        by key-value pairs in *where_data*. Care should be exercised if *where_clause* contains *IN* directives.
        In PostgreSQL, the list of values for an attribute with the *IN* directive must be contained in a
        specific tuple, and the operation will break for a list of values containing only 1 element.
        The safe way to specify *IN* directives is to add them to *where_data*, as the specifics of each
        DB flavor will then be properly dealt with.

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
        :param errors: incidental error messages
        :param logger: optional logger
        :return: the number of deleted tuples, or *None* if error
        """
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
    def store(insert_data: dict[str, Any] = None,
              return_cols: dict[str, Any] = None,
              db_engine: DbEngine = None,
              db_conn: Any = None,
              committable: bool = None,
              errors: list[str] = None,
              logger: Logger = None) -> tuple | int | None:
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
        :param errors: incidental error messages
        :param logger: optional logger
        :return: the values of *return_cols*, the number of inserted tuples (0 ou 1), or *None* if error
        """
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
    def __get_invoking_class(errors: list[str] | None,
                             logger: Logger | None) -> type[Sob] | None:
        """
        Retrieve the fully-qualified type of the subclass currently being accessed.

        :param errors: incidental error messages
        :param logger: optional logger
        :return: the fully-qualified type of the subclass currently being accessed
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
                        f"filepath '{filepath}', from invoking function '{invoking_function}'")
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
                          committable: bool | None,
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
                                    committable=committable,
                                    errors=errors)
                if errors:
                    break

    # noinspection PyPep8
    @staticmethod
    def __build_from_clause(alias: str,
                            cls: type[Sob],
                            joins: list[tuple[type[Sob] | str, str, str]] |
                                   list[tuple[type[Sob] | str, str, str,
                                              Literal["inner", "full", "left", "right"]]] | None) -> str:

        # obtain the the fully-qualified name of the class type
        cls_name: str = f"{cls.__module__}.{cls.__qualname__}"

        # establish the main query table
        result: str = sob_db_specs[cls_name][0]
        if alias:
            result += " AS " + alias

        # build the joins
        for join in joins or []:
            if isinstance(join[0], str):
                target: str = join[0]
            else:
                name: str = f"{join[0].__module__}.{join[0].__qualname__}"
                target: str = sob_db_specs[name][0]
            mode: str = join[3].upper() if len(join) > 3 else "INNER"
            result += f" {mode} JOIN {target} AS {join[1]} ON {join[2]}"

        return result
