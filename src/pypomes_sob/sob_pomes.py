from __future__ import annotations  # allow forward references
from inspect import FrameInfo, stack
from enum import Enum, StrEnum
from logging import Logger
from pypomes_core import dict_get_key, dict_stringify
from pypomes_db import (
    db_exists, db_count, db_select,
    db_insert, db_update, db_delete
)
# 'type' is not a viable replacement for 'typing.Type', because it does not accept subclasses
from typing import Any, ClassVar, Final, Type  # noqa


# noinspection Annotator
# ruff: noqa: UP006
class PySob:
    """
    Root entity.
    """
    _db_specs: ClassVar[dict[Type[PySob], (StrEnum | str, StrEnum | str)]] = {}

    _attrs_map: ClassVar[dict[Type[PySob], dict[StrEnum | str, StrEnum | str]]] = {}

    _sob_references: ClassVar[dict[Type[PySob], list[Type[PySob]]]] = {}

    def __init__(self,
                 errors: list[str] = None,
                 load_references: bool = False,
                 where_data: dict[str, Any] = None,
                 db_conn: Any = None,
                 logger: Logger = None) -> None:

        self._logger: Logger = logger
        self.id: int = 0

        if where_data:
            attrs: list[str] = self.get_columns()
            recs: list[tuple] = db_select(errors=errors,
                                          sel_stmt=f"SELECT {', '.join(attrs)} "
                                                   f"FROM {PySob._db_specs[self.__class__][0]}",
                                          where_data=where_data,
                                          limit_count=2,
                                          connection=db_conn,
                                          logger=self._logger)
            msg: str | None = None
            if errors:
                msg = ("Error SELECTing from table "
                       f"{PySob._db_specs[self.__class__][0]}: {'; '.join(errors)}")
            elif not recs:
                msg = (f"No record found on table "
                       f"{PySob._db_specs[self.__class__][0]} for {dict_stringify(where_data)}")
            elif len(recs) > 1:
                msg = (f"More than on record found on table "
                       f"{PySob._db_specs[self.__class__][0]} for {dict_stringify(where_data)}")
            if msg:
                errors.append(msg)
                if self._logger:
                    self._logger.error(msg=msg)
            else:
                rec: tuple = recs[0]
                for inx, attr in enumerate(attrs):
                    self.__dict__[attr] = rec[inx]
                if load_references:
                    self.__load_references(errors=errors,
                                           db_conn=db_conn)

    def insert(self,
               errors: list[str] | None,
               db_conn: Any = None) -> bool:

        # prepara data for INSERT
        insert_data: dict[str, Any] = self.to_columns(omit_nulls=False)

        # execute the INSERT statement
        errors = errors if isinstance(errors, list) else []
        rec: tuple[int] = db_insert(errors=errors,
                                    insert_stmt=f"INSERT INTO {PySob._db_specs[self.__class__][0]}",
                                    insert_data=insert_data,
                                    return_cols={PySob._db_specs[self.__class__][1]: int},
                                    connection=db_conn,
                                    logger=self._logger)
        if errors:
            msg = ("Error INSERTing into table "
                   f"{PySob._db_specs[self.__class__][0]}: {'; '.join(errors)}")
            errors.append(msg)
            if self._logger:
                self._logger.error(msg=msg)
        else:
            self.id = rec[0]

        return not errors

    def update(self,
               errors: list[str] | None,
               load_references: bool = False,
               db_conn: Any = None) -> bool | None:

        # inicialize the return variable
        result: bool | None = None

        # prepare data for UPDATE
        update_data: dict[str, Any] = self.to_columns(omit_nulls=False)
        key: int = update_data.pop(PySob._db_specs[self.__class__][1])

        # execute the UPDATE statement
        errors = errors if isinstance(errors, list) else []
        db_update(errors=errors,
                  update_stmt=f"UPDATE {PySob._db_specs[self.__class__][0]}",
                  update_data=update_data,
                  where_data={PySob._db_specs[self.__class__][1]: key},
                  min_count=1,
                  max_count=1,
                  connection=db_conn,
                  logger=self._logger)

        if not errors and load_references:
            self.__load_references(errors=errors,
                                   db_conn=db_conn)
        if errors:
            msg = ("Error UPDATEing table "
                   f"{PySob._db_specs[self.__class__][0]}: {'; '.join(errors)}")
            errors.append(msg)
            if self._logger:
                self._logger.error(msg=msg)
        else:
            result = True

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
        result: int = db_delete(errors=errors,
                                delete_stmt=f"DELETE FROM {PySob._db_specs[self.__class__][0]}",
                                where_data=where_data,
                                max_count=1,
                                connection=db_conn,
                                logger=self._logger)
        if errors:
            msg = ("Error DELETEing from table "
                   f"{PySob._db_specs[self.__class__][0]}: {'; '.join(errors)}")
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

    def exists(self,
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

        if not where_data:
            if self.id:
                where_data = {PySob._db_specs[self.__class__][1]: self.id}
            else:
                where_data = self.to_columns(omit_nulls=omit_nulls)
                where_data.pop(PySob._db_specs[self.__class__][1], None)

        # it is acceptable that loading the object from the database might fail
        attrs: list[str] = self.get_columns()
        errors = errors if isinstance(errors, list) else []
        recs: list[tuple] = db_select(errors=errors,
                                      sel_stmt=f"SELECT {', '.join(attrs)} "
                                               f"FROM {PySob._db_specs[self.__class__][0]}",
                                      where_data=where_data,
                                      limit_count=2,
                                      connection=db_conn,
                                      logger=self._logger)
        if recs and len(recs) == 1:
            rec: tuple = recs[0]
            for inx, attr in enumerate(attrs):
                self.__dict__[attr] = rec[inx]
        elif self._logger:
            self._logger.warning(msg=f"Unable to load from table "
                                     f"{PySob._db_specs[self.__class__][0]} "
                                     f"with data {dict_stringify(where_data)}")

        return recs and len(recs) == 1

    def get_columns(self) -> list[str]:

        return [k for k in self.__dict__
                if k.islower() and not k.startswith("_")]

    def to_columns(self,
                   omit_nulls: bool) -> dict[str, Any]:

        return {k: v for k, v in self.__dict__.items()
                if k.islower() and not k.startswith("_") and (not omit_nulls or v is not None)}

    def to_params(self,
                  omit_nulls: bool) -> dict[str, Any]:

        result: dict[str, Any] = {}
        for key, value in self.__dict__.items():
            if key.islower() and not key.startswith("_") and \
                    (not omit_nulls or value is not None):
                attr: str = dict_get_key(source=PySob._attrs_map.get(self.__class__) or {},
                                         value=key) or key
                result[attr] = value

        return result

    def data_to_params(self,
                       data: dict[str, Any],
                       omit_nulls: bool) -> dict[str, Any]:

        result: dict[str, Any] = {}
        for key, value in data.items():
            if not omit_nulls or value is not None:
                attr: str = dict_get_key(source=PySob._attrs_map.get(self.__class__) or {},
                                         value=key) or key
                result[attr] = value

        return result

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def load_reference(self,
                       __cls: Type[PySob],
                       /,
                       errors: list[str] | None,  # noqa: ARG002
                       db_conn: Any | None) -> PySob | list[PySob] | None:  # noqa: ARG002

        # to be implemented by subclasses containing references
        return None

    def __load_references(self,
                          errors: list[str],
                          db_conn: Any) -> None:

        for cls in (PySob._sob_references.get(self.__class__) or []):
            self.load_reference(cls,
                                errors=errors,
                                db_conn=db_conn)
            if errors:
                msg = (f"Error SELECTing from table "
                       f"{PySob._db_specs[cls][0]}: {'; '.join(errors)}")
                errors.append(msg)
                if self._logger:
                    self._logger.error(msg=msg)
                break

    @staticmethod
    def initialize(__cls: Type[PySob],
                   /,
                   db_specs: (StrEnum | str, StrEnum | str),
                   attrs_map: dict[StrEnum | str, StrEnum | str] = None,
                   sob_references: list[Type[PySob]] = None) -> None:

        # initialize the class
        PySob._db_specs.update({__cls: db_specs})
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
        cls: Type[PySob] = PySob.__get_invoking_class()

        return db_count(errors=errors,
                        table=PySob._db_specs[cls][0],
                        where_data=where_data,
                        connection=db_conn,
                        logger=logger)

    @staticmethod
    def retrieve(errors: list[str] | None,
                 where_data: dict[str, Any] = None,
                 load_references: bool = False,
                 required: bool = False,
                 db_conn: Any = None,
                 logger: Logger = None) -> list[PySob] | None:

        # inicialize the return variable
        result: list[PySob] | None = None

        # obtain rtheinvoking class
        cls: Type[PySob] = PySob.__get_invoking_class()

        errors = errors if isinstance(errors, list) else []
        recs: list[tuple[int]] = db_select(errors=errors,
                                           sel_stmt=f"SELECT {PySob._db_specs[cls][1]} "
                                                    f"FROM {PySob._db_specs[cls][0]}",
                                           where_data=where_data,
                                           min_count=1 if required else None,
                                           connection=db_conn,
                                           logger=logger)
        if not errors:
            # build the objects list
            objs: list[PySob] = []
            for rec in recs:
                objs.append(cls(errors=errors,
                                load_references=load_references,
                                db_conn=db_conn,
                                where_data={PySob._db_specs[cls][1]: rec[0]},
                                logger=logger))
                if errors:
                    break

            if not errors:
                result = objs

        return result

    @staticmethod
    def erase(errors: list[str] | None,
              where_data: dict[str, Any],
              db_conn: Any = None,
              logger: Logger = None) -> int | None:

        # obtain the invoking class
        cls: Type[PySob] = PySob.__get_invoking_class()

        # delete specified tuples
        return db_delete(errors=errors,
                         delete_stmt=f"DELETE FROM {PySob._db_specs[cls][0]}",
                         where_data=where_data,
                         connection=db_conn,
                         logger=logger)

    @staticmethod
    def __get_invoking_class() -> Type[PySob]:

        # obtain the invoking function
        caller_frame: FrameInfo = stack()[1]
        mark: str = f".{caller_frame.function}("

        # obtain the invoking class
        caller_frame = stack()[2]
        context: str = caller_frame.code_context[0]
        pos_to: int = context.find(mark)
        pos_from: int = context.rfind(" ", 0, pos_to)
        mark = "." + context[pos_from+1:pos_to]

        result: Type[PySob] | None = None
        for cls in PySob._db_specs:
            name = f"{cls.__module__}.{cls.__qualname__}"
            if name.endswith(mark):
                result = cls
                break

        return result
