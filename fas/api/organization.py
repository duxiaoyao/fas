import logging

from asyncpg import Connection
from fastapi import Depends, APIRouter

from fas.model import list_organizations
from .db import db_connection

LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get("/")
async def list_organizations_(db_conn: Connection = Depends(db_connection)):
    organizations = await list_organizations(db_conn)
    LOGGER.debug(f'{organizations}')
    return organizations
