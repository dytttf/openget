# coding:utf8
import logging
import os
import sys
import platform
import tempfile
import logging.handlers
from better_exceptions import format_exception


# TODO support env
DEFAULT_LOG_FMT = "[%(threadName)s] [%(asctime)s] [%(levelname)s] [%(filename)s] [%(lineno)d] - %(message)s"


def detect_tmp_log_dir() -> str:
    """
        Get
    Returns:
        *unix: /var/log/graper_log/
        mac: /tmp/graper_log
        windows: tempfile.gettempdir()

    """
    system = platform.system()
    if system == "Windows":
        log_dir = tempfile.gettempdir()
    elif system == "Darwin":
        log_dir = "/tmp/"
    else:
        log_dir = "/var/log/"

    # TODO support env
    log_dir = os.path.join(log_dir, "graper_log")
    os.makedirs(log_dir, exist_ok=True)
    return log_dir


def get_logger(
    name,
    log_level: str = "DEBUG",
    local: bool = False,
    local_log_dir: str = None,
    log_format=DEFAULT_LOG_FMT,
):
    """

    Args:
        name:
        log_level:
        local:
        local_log_dir:
        log_format:

    Returns:

    """
    name = name.split(os.sep)[-1].split(".")[0]

    _logger = logging.getLogger(name)
    _logger.setLevel(log_level)

    #
    def _format_exception(exc_info):
        exc_str = format_exception(*exc_info)
        return "\n".join(exc_str) if not isinstance(exc_str, str) else exc_str

    #
    formatter = logging.Formatter(log_format)
    if os.getenv("GRAPER_FORBIDDED_BETTER_EXCEPTIONS", None) is None:
        formatter.formatException = _format_exception

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(formatter)
    # check duplicate
    handle_exists = 0
    for _handler in _logger.handlers:
        if (
            isinstance(_handler, logging.StreamHandler)
            and _handler.stream == sys.stdout
        ):
            handle_exists = 1
    if not handle_exists:
        _logger.addHandler(stream_handler)

    #
    if local:
        local_log_dir = local_log_dir or detect_tmp_log_dir()
        rotating_file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(local_log_dir, f"{name}.log"),
            maxBytes=100 * 1024 * 1024,
            backupCount=10,
            encoding="utf-8",
        )
        rotating_file_handler.setFormatter(formatter)
        _logger.addHandler(rotating_file_handler)

    return _logger


if __name__ == "__main__":
    logger = get_logger("xxx", local=False)
    logger.debug("test")

    try:
        a = 1
        b = 0
        c = a / b
    except Exception as e:
        logger.exception(e)
