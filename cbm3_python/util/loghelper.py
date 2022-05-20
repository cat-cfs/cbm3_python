# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import logging


def start_logging(fn, fmode="w", use_console=True):
    """Start logging to a file and optionally to the console window

    Args:
        fn (str): Path to the log file.
        fmode (str, optional): File mode string passed to logging.FileHandler.
            Defaults to 'w'.
        use_console (bool, optional): If set to true the logging output
            is written to console. Defaults to True.
    """
    rootLogger = logging.getLogger()

    logFormatter = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
        datefmt="%m-%d %H:%M",
    )

    fileHandler = logging.FileHandler(fn, fmode)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)
    if use_console:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        rootLogger.addHandler(consoleHandler)

    rootLogger.setLevel(logging.INFO)


def get_logger():
    """Gets the "cbm3_python" logger

    Returns:
        Logger: the "cbm3_python" logging.Logger
    """
    return logging.getLogger("cbm3_python")
