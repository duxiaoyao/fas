import logging
from typing import List

from fastapi import Depends, APIRouter

from fas.model import Organization, list_organizations
from fas.util.database import Connection
from .db import get_db

LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get('/', response_model=List[Organization])
async def list_organizations_(db: Connection = Depends(get_db)):
    return await list_organizations(db)
