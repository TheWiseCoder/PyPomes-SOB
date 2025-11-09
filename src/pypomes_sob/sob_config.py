from pypomes_core import APP_PREFIX, env_get_int, env_get_str
from typing import Final

# base folder name for all 'PySob' subclasses files (must be a part of a Posix-compliant path)
SOB_BASE_FOLDER: Final[str] = env_get_str(key=f"{APP_PREFIX}_SOB_BASE_FOLDER",
                                          def_value="entities")
# maximum number of threads to use
SOB_MAX_THREADS: Final[int] = env_get_int(key=f"{APP_PREFIX}_SOB_MAX_THREADS",
                                          def_value=1)

# must have entries for all subclasses of 'PySob'
#   key: the fully-qualified name of the class type of the subclass of 'PySob'
#   values: the names of the entity's database columns, which must be matched by the names of its attributes
#           (the first element is the name of its PK attribute - maps to 'self.id')
sob_db_columns: dict[str, tuple] = {}

# must have entries for all subclasses of 'PySob'
#   key: the fully-qualified name of the class type of the subclass of 'PySob'
#   value: a tuple with 3 elements:
#     - the name of the entity's DB table
#     - the type of its PK attribute (currently, 'int' and 'str' are supported)
#     - whether the PK attribute is an identity (has values generated automatically by the DB)
sob_db_specs: dict[str, tuple[str, type, bool]] = {}

# holds sets of instance attributes unique in DB
sob_attrs_unique: dict[str, list[tuple[str]]] = {}

# lists names for data input, mapping them to instance attributes (may map to 'None')
sob_attrs_input: dict[str, list[tuple[str, str]]] = {}
