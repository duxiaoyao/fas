from typing import List

from fastapi import Depends, APIRouter

from fas.model import Organization, list_organizations
from fas.util.database import DBConnection
from .db import acquire_db

router = APIRouter()


@router.get('/', response_model=List[Organization])
async def list_organizations_(db: DBConnection = Depends(acquire_db)):
    return await list_organizations(db)
