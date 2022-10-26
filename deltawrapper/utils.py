import logging


def get_logger(name: str, log_level="INFO") -> logging.Logger:
    """Get a logger.
    Args:
        name (str): The name of the logger.
        log_level (str): The log level. Defaults to "INFO".
    Returns:
        logging.Logger: The logger.
    """
    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        level=logging.getLevelName(log_level),
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler()],
    )
    logging.getLogger("py4j").setLevel(logging.ERROR)
    return logging.getLogger(name)
