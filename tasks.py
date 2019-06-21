import logging
import sys

from invoke import Collection

from fas.util.database.tasks import db_tasks

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

ns = Collection(db_tasks)
