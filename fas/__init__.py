import logging

from dynaconf import settings

if settings.DEBUG:
    logging.getLogger(__name__).setLevel(logging.DEBUG)
