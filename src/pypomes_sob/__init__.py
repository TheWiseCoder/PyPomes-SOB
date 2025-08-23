from .sob_pomes import PySob, Sob

__all__ = [
    "PySob", "Sob"
]

from importlib.metadata import version
__version__: str = version("pypomes_sob")
__version_info__: tuple = tuple(int(i) for i in __version__.split(".") if i.isdigit())
