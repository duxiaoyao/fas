import logging
import sys

from invoke import Collection

from fas.util.database.tasks import db_tasks
from tests.tasks import test

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

ns = Collection(db_tasks, test)
