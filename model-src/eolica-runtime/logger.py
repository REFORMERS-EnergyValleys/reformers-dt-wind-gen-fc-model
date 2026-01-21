import logging

def enable_default_logging(level=logging.INFO):
    """
    Turn on logging to stdout.
    :param level integer: The logging level you wish to use.
                          Defaults to logging.INFO.
    """
    import sys
    import logging
    logger = logging.getLogger('eolica-runtime')
    handler_names = [handler.name for handler in logger.handlers]
    if 'eolica_runtime_default_handler' not in handler_names:
        logger.setLevel(level)
        logging_handler = logging.StreamHandler(stream=sys.stdout)
        logging_handler.set_name('eolica_runtime_default_handler')
        logging_handler.setLevel(logging.DEBUG)
        logger.addHandler(logging_handler)

# enable_default_logging(level=logging.INFO)
enable_default_logging(level=logging.DEBUG)

Logger = logging.getLogger('eolica-runtime')