import dynaconf

if dynaconf.settings.DEBUG:
    import logging

    logging.getLogger(__name__).setLevel(logging.DEBUG)
