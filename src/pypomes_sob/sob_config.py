from enum import StrEnum
from pypomes_core import APP_PREFIX, env_get_str
from typing import Final

# base folder name for all 'PySob' subclasses files (must be a part of a Posix-compliant path)
SOB_BASE_FOLDER: Final[str] = env_get_str(key=f"{APP_PREFIX}_SOB_BASE_FOLDER")

# must have entries for all subclasses of 'PySob':
#   key: the fully-qualified name of the class type of the subclass of 'PySob'
#   value: a tuple with 4 elements:
#     - the name of the entity's DB table
#     - the name of its PK attribute (maps to 'self.id')
#     - the type of its PK attribute (currently, 'int' and 'str' are supported)
#     - whether the PK attribute is an identity (has values generated automatically by the DB)
sob_db_specs: dict[str, (StrEnum | str, StrEnum | str, type, bool)] = {}

# maps input parameters to DB columns
sob_attrs_map: dict[str, dict[StrEnum | str, StrEnum | str]] = {}

# holds 'PySob' fully-qualified names of subclasses referred to by the current class
sob_cls_references: dict[str, list[str]] = {}
