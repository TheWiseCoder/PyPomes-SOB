from __future__ import annotations  # allow forward references
from inspect import FrameInfo, stack
from enum import Enum, StrEnum
from logging import Logger
from pypomes_core import dict_get_key, dict_stringify
from pypomes_db import (
    db_exists, db_count, db_select,
    db_insert, db_update, db_delete
)
from typing import Any, TypeVar

# 'Sob' stands for all subclasses of 'PySob'
Sob = TypeVar("Sob",
              bound="PySob")


class PySob:
    """
    Root entity.
    """
    # must have entries for all subclasses of 'PySob':
    #   key: the class type of the subclass of 'PySob'
    #   value: a tuple with 4 elements:
    #     - the name of the entity's DB table
    #     - the name of its PK attribute (maps to 'self.id')
    #     - the type of its PK attribute (currently, 'int' and 'str' are supported)
    #     - whether the PK attribute is an identity (has values generated automatically by the DB)
    _db_specs: dict[type[Sob], (StrEnum | str, StrEnum | str, type, bool)] = {}

    # maps input parameters to DB columns
    _attrs_map: dict[type[Sob], dict[StrEnum | str, StrEnum | str]] = {}

    # holds 'PySob' subclasses referred to by the current class
    _sob_references: dict[type[Sob], list[type[Sob]]] = {}

    def __init__(self,
                 errors: list[str] = None,
                 load_references: bool = False,
                 where_data: dict[str, Any] = None,
                 db_conn: Any = None,
                 logger: Logger = None) -> None:

        self._logger: Logger = logger
        # maps to the entity's PK in its DB table (returned on INSERT operations)
        self.id: int | str = 0

        # determine whether the instance exists in the database
        self._is_new: bool = True

        if where_data:
            self.load(errors=errors,
                      omit_nulls=True,
                      where_data=where_data)
        if not errors and load_references:
            self.__load_references(errors=errors,
                                   db_conn=db_conn)

    def insert(self,
               errors: list[str] | None,
               db_conn: Any = None) -> bool:

        # prepara data for INSERT
        return_col: dict[str, type] | None = None
        insert_data: dict[str, Any] = self.to_columns(omit_nulls=True)
        if PySob._db_specs[self.__class__][3]:
            # PK is as identity column
            insert_data.pop(PySob._db_specs[self.__class__][1], None)
            return_col = {PySob._db_specs[self.__class__][1]: PySob._db_specs[self.__class__][2]}

        # execute the INSERT statement
        op_errors: list[str] = []
        rec: tuple[Any] = db_insert(errors=op_errors,
                                    insert_stmt=f"INSERT INTO {PySob._db_specs[self.__class__][0]}",
                                    insert_data=insert_data,
                                    return_cols=return_col,
                                    connection=db_conn,
                                    logger=self._logger)
        if op_errors:
            msg = ("Error INSERTing into table "
                   f"{PySob._db_specs[self.__class__][0]}: {'; '.join(op_errors)}")
            if isinstance(errors, list):
                errors.append(msg)
            if self._logger:
                self._logger.error(msg=msg)
        else:
            self._is_new = False
            if PySob._db_specs[self.__class__][3]:
                # PK is as identity column
                self.id = rec[0]

        return not op_errors

    def update(self,
               errors: list[str] | None,
               db_conn: Any = None) -> bool:

        # prepare data for UPDATE
        update_data: dict[str, Any] = self.to_columns(omit_nulls=False)
        key: int = update_data.pop(PySob._db_specs[self.__class__][1])

        # execute the UPDATE statement
        op_errors: list[str] = []
        db_update(errors=op_errors,
                  update_stmt=f"UPDATE {PySob._db_specs[self.__class__][0]}",
                  update_data=update_data,
                  where_data={PySob._db_specs[self.__class__][1]: key},
                  min_count=1,
                  max_count=1,
                  connection=db_conn,
                  logger=self._logger)
        if op_errors:
            msg: str = ("Error UPDATEing table "
                        f"{PySob._db_specs[self.__class__][0]}: {'; '.join(op_errors)}")
            if isinstance(errors, list):
                errors.append(msg)
            if self._logger:
                self._logger.error(msg=msg)

        return not op_errors

    def persist(self,
                errors: list[str] | None,
                db_conn: Any = None) -> bool:

        # declare the return variable
        result: bool

        if self._is_new:
            result = self.insert(errors=errors,
                                 db_conn=db_conn)
        else:
            result = self.update(errors=errors,
                                 db_conn=db_conn)
        return result

    def delete(self,
               errors: list[str] | None,
               db_conn: Any = None) -> int | None:

        where_data: dict[str, Any]
        if self.id:
            where_data = {PySob._db_specs[self.__class__][1]: self.id}
        else:
            where_data = self.to_columns(omit_nulls=True)
            where_data.pop(PySob._db_specs[self.__class__][1], None)

        # execute the DELETE statement
        op_errors: list[str] = []
        result: int = db_delete(errors=op_errors,
                                delete_stmt=f"DELETE FROM {PySob._db_specs[self.__class__][0]}",
                                where_data=where_data,
                                max_count=1,
                                connection=db_conn,
                                logger=self._logger)
        if op_errors:
            msg = ("Error DELETEing from table "
                   f"{PySob._db_specs[self.__class__][0]}: {'; '.join(op_errors)}")
            if isinstance(errors, list):
                errors.append(msg)
            if self._logger:
                self._logger.error(msg=msg)
        else:
            self.clear()

        return result

    def clear(self) -> None:

        for key in self.__dict__:
            self.__dict__[key] = None

    def set(self,
            data: dict[str, Any]) -> None:

        for key, value in data.items():
            attr: str = (PySob._attrs_map.get(self.__class__) or {}).get(key) or key

            # usa nomes de enums atribuídos como valores em 'data'
            if isinstance(value, Enum) and "use_names" in value.__class__:
                value = value.name  # noqa: PLW2901

            if attr in self.__dict__:
                self.__dict__[attr] = value
            elif self._logger:
                self._logger.warning(msg=f"'{attr}'is not an attribute of "
                                         f"{PySob._db_specs[self.__class__][0]}")

    def in_db(self,
              errors: list[str] | None,
              where_data: dict[str, Any] = None,
              db_conn: Any = None) -> bool | None:

        if not where_data:
            if self.id:
                # use object's ID
                where_data = {PySob._db_specs[self.__class__][1]: self.id}
            else:
                # use object's available data
                where_data = self.to_columns(omit_nulls=True)
                where_data.pop(PySob._db_specs[self.__class__][1], None)

        return db_exists(errors=errors,
                         table=PySob._db_specs[self.__class__][0],
                         where_data=where_data,
                         connection=db_conn,
                         logger=self._logger)

    def load(self,
             errors: list[str] | None,
             omit_nulls: bool,
             where_data: dict[str, Any] = None,
             db_conn: Any = None) -> bool:

        # initialize the return variable
        result: bool = False

        if not where_data:
            if self.id:
                where_data = {PySob._db_specs[self.__class__][1]: self.id}
            else:
                where_data = self.to_columns(omit_nulls=omit_nulls)
                where_data.pop(PySob._db_specs[self.__class__][1], None)

        # loading the object from the database might fail
        attrs: list[str] = self.get_columns()
        op_errors: list[str] = []
        recs: list[tuple] = db_select(errors=op_errors,
                                      sel_stmt=f"SELECT {', '.join(attrs)} "
                                               f"FROM {PySob._db_specs[self.__class__][0]}",
                                      where_data=where_data,
                                      limit_count=2,
                                      connection=db_conn,
                                      logger=self._logger)
        msg: str | None = None
        if op_errors:
            msg = ("Error SELECTing from table "
                   f"{PySob._db_specs[self.__class__][0]}: {'; '.join(op_errors)}")
        elif not recs:
            msg = (f"No record found on table "
                   f"{PySob._db_specs[self.__class__][0]} for {dict_stringify(where_data)}")
        elif len(recs) > 1:
            msg = (f"More than on record found on table "
                   f"{PySob._db_specs[self.__class__][0]} for {dict_stringify(where_data)}")

        if msg:
            if isinstance(errors, list):
                errors.append(msg)
            if self._logger:
                self._logger.error(msg=msg)
        else:
            rec: tuple = recs[0]
            for inx, attr in enumerate(attrs):
                # PK attribute in DB table might have a different name
                if attr == PySob._db_specs[self.__class__][0]:
                    self.__dict__["id"] = rec[inx]
                else:
                    self.__dict__[attr] = rec[inx]
            self._is_new = False
            result = True

        return result

    def get_columns(self) -> list[str]:

        # PK attribute in DB table might have a different name
        result: list[str] = [PySob._db_specs[self.__class__][1]]
        result.extend([k for k in self.__dict__
                      if k.islower() and not k.startswith("_") and not k == "id"])
        return result

    def to_columns(self,
                   omit_nulls: bool) -> dict[str, Any]:

        # PK attribute in DB table might have a different name
        result: dict[str, Any] = {PySob._db_specs[self.__class__][1]: self.__dict__.get("id")}
        result.update({k: v for k, v in self.__dict__.items()
                      if k.islower() and not (k.startswith("_") or k == "id" or (omit_nulls and v is None))})
        return result

    def to_params(self,
                  omit_nulls: bool) -> dict[str, Any]:

        return self.data_to_params(data=self.__dict__,
                                   omit_nulls=omit_nulls)

    def data_to_params(self,
                       data: dict[str, Any],
                       omit_nulls: bool) -> dict[str, Any]:

        result: dict[str, Any] = {}
        for k, v in data.items():
            if not omit_nulls or v is not None:
                attr: str = dict_get_key(source=PySob._attrs_map.get(self.__class__) or {},
                                         value=k) or k
                result[attr] = v

        return result

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def load_reference(self,
                       __cls: type[Sob],
                       /,
                       errors: list[str] | None,
                       db_conn: Any | None) -> Sob | list[Sob] | None:  # noqa: ARG002

        # must be implemented by subclasses containing references
        msg: str = f"Subclass {__cls.__module__}.{__cls.__qualname__} failed to implement 'load_reference()'"
        if isinstance(errors, list):
            errors.append(msg)
        if self._logger:
            self._logger.error(msg=msg)

        return None

    def __load_references(self,
                          errors: list[str] | None,
                          db_conn: Any) -> None:

        op_errors: list[str] = []
        for cls in (PySob._sob_references.get(self.__class__) or []):
            self.load_reference(cls,
                                errors=op_errors,
                                db_conn=db_conn)
            if op_errors:
                msg = (f"Error SELECTing from table "
                       f"{PySob._db_specs[cls][0]}: {'; '.join(op_errors)}")
                if isinstance(errors, list):
                    errors.append(msg)
                if self._logger:
                    self._logger.error(msg=msg)
                break

    @staticmethod
    def initialize(__cls: type[Sob],
                   /,
                   db_specs: tuple[StrEnum | str, StrEnum | str, int | str] |
                             tuple[StrEnum | str, StrEnum | str, int, bool],  # noqa
                   attrs_map: dict[StrEnum | str, StrEnum | str] = None,
                   sob_references: list[type[Sob]] = None) -> None:

        # initialize the class
        specs: list = list(db_specs)
        if len(specs) == 3:
            # 'id' defaults to being an identity attribute in the DB for type 'int'
            specs.append(specs[2] is int)
        PySob._db_specs.update({__cls: tuple(specs)})
        if attrs_map:
            PySob._attrs_map.update({__cls: attrs_map})
        if sob_references:
            PySob._sob_references.update({__cls: sob_references})

    @staticmethod
    def count(errors: list[str] | None,
              where_data: dict[str, Any],
              db_conn: Any = None,
              logger: Logger = None) -> int | None:

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class()

        return db_count(errors=errors,
                        table=PySob._db_specs[cls][0],
                        where_data=where_data,
                        connection=db_conn,
                        logger=logger)

    @staticmethod
    def exists(errors: list[str] | None,
               where_data: dict[str, Any],
               db_conn: Any = None,
               logger: Logger = None) -> int | None:

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class()

        return db_exists(errors=errors,
                         table=PySob._db_specs[cls][0],
                         where_data=where_data,
                         connection=db_conn,
                         logger=logger)

    @staticmethod
    def retrieve(errors: list[str] | None,
                 where_data: dict[str, Any] = None,
                 load_references: bool = False,
                 min_count: int = None,
                 max_count: int = None,
                 limit_count: int = None,
                 db_conn: Any = None,
                 logger: Logger = None) -> list[Sob] | None:

        # inicialize the return variable
        result: list[Sob] | None = None

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class()

        op_errors: list[str] = []
        recs: list[tuple[int | str]] = db_select(errors=op_errors,
                                                 sel_stmt=f"SELECT {PySob._db_specs[cls][1]} "
                                                          f"FROM {PySob._db_specs[cls][0]}",
                                                 where_data=where_data,
                                                 min_count=min_count,
                                                 max_count=max_count,
                                                 limit_count=limit_count,
                                                 connection=db_conn,
                                                 logger=logger)
        if not op_errors:
            # build the objects list
            objs: list[Sob] = []
            for rec in recs:
                # constructor of 'cls', a subclass of 'PySob', takes slightly different arguments
                objs.append(cls(rec[0],
                                errors=op_errors,
                                load_references=load_references,
                                db_conn=db_conn,
                                logger=logger))
                if op_errors:
                    break

            if not op_errors:
                result = objs

        if op_errors:
            msg = "; ".join(op_errors)
            if isinstance(errors, list):
                errors.append(msg)
            if logger:
                logger.error(msg=msg)

        return result

    @staticmethod
    def erase(errors: list[str] | None,
              where_data: dict[str, Any],
              db_conn: Any = None,
              logger: Logger = None) -> int | None:

        # obtain the invoking class
        cls: type[Sob] = PySob.__get_invoking_class()

        # delete specified tuples
        op_errors: list[str] = []
        result: int = db_delete(errors=op_errors,
                                delete_stmt=f"DELETE FROM {PySob._db_specs[cls][0]}",
                                where_data=where_data,
                                connection=db_conn,
                                logger=logger)
        if op_errors:
            msg = "; ".join(op_errors)
            if isinstance(errors, list):
                errors.append(msg)
            if logger:
                logger.error(msg=msg)

        return result

    @staticmethod
    def __get_invoking_class() -> type[Sob]:

        # obtain the invoking function
        caller_frame: FrameInfo = stack()[1]
        mark: str = f".{caller_frame.function}("

        # obtain the invoking class
        caller_frame = stack()[2]
        context: str = caller_frame.code_context[0]
        pos_to: int = context.find(mark)
        pos_from: int = context.rfind(" ", 0, pos_to)
        mark = "." + context[pos_from+1:pos_to]

        result: type[Sob] | None = None
        for cls in PySob._db_specs:
            name = f"{cls.__module__}.{cls.__qualname__}"
            if name.endswith(mark):
                result = cls
                break

        return result
