import logging
import os
from datetime import datetime

import pytz
from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme

CONSOLE_THEMES = Theme({"info": "dim cyan", "warning": "magenta", "danger": "bold red"})
log_levels = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}
log_format = "%(filename)s %(name)s %(message)s"
date_time_format = "[%Y/%m/%d %H:%M:%S]"
time_zone = pytz.utc


class LoggingFormatter(logging.Formatter):
    """Overwrites the logging formatter properties."""

    def __init__(self) -> None:
        """Overwrites the formatter's default date and time format and time zone."""
        super().__init__(log_format, date_time_format)
        self.tz = time_zone
        self.datefmt = date_time_format

    def formattime(self, record: logging.LogRecord, datefmt: str) -> str:
        """Overwrites the record's default time format."""
        date_time = datetime.fromtimestamp(record.created, self.tz)
        return date_time.strftime(self.datefmt)

    def format(self, record: logging.LogRecord) -> logging.LogRecord:
        """Defines new format of record.
        Things updated:
        * Time structure to include date and time at pre-determined time_zone.
        * Record filename.
        * Formats record source.
        * Component widths.
        """
        record.asctime = self.formattime(record, self.datefmt)
        component_width = 30
        file_lineno = f"{record.filename}:{record.lineno}"
        record.filename = file_lineno.ljust(component_width)[:component_width]
        component_width = 45
        name_colon = f"{record.name}:"
        if name_colon.startswith("drunc."):
            name_colon = name_colon.replace("drunc.", "")
        record.name = name_colon.ljust(component_width)[:component_width]
        component_width = 10
        level_name = record.levelname
        record.levelname = level_name.ljust(component_width)[:component_width]
        return super().format(record)


def construct_logger(name: str) -> logging.Logger:
    """Contruct standard logger."""
    try:
        width = os.get_terminal_size()[0]
    except OSError:
        width = 150

    handler = RichHandler(
        console=Console(width=width),
        omit_repeated_times=False,
        markup=True,
        rich_tracebacks=True,
        show_path=False,
        tracebacks_width=width,
    )
    handler.setFormatter(LoggingFormatter())
    log = logging.getLogger(name)
    log.addHandler(handler)
    return log
