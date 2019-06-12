from typing import List

from fastapi import Depends, APIRouter, Body, HTTPException
from starlette.status import HTTP_201_CREATED

from fas.model import *
from fas.util.database import DBClient
from .db import acquire_db

router = APIRouter()


@router.get('/', response_model=List[Organization])
async def list_organizations_(db: DBClient = Depends(acquire_db)):
    return await list_organizations(db)


@router.post('/', response_model=Organization, status_code=HTTP_201_CREATED)
async def create_organization_(name: str = Body(..., min_length=2, max_length=32), db: DBClient = Depends(acquire_db)):
    return await create_organization(db, name)


@router.get('/{id}', response_model=Organization)
async def get_organization_(id: int, db: DBClient = Depends(acquire_db)):
    org = await get_organization(db, id)
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return org


@router.put('/{id}')
async def update_organization_(id: int, name: str = Body(..., min_length=2, max_length=32),
                               db: DBClient = Depends(acquire_db)):
    rowcount = await update_organization(db, id, name)
    if rowcount == 0:
        raise HTTPException(status_code=404, detail="Organization not found")
    return rowcount


@router.delete('/{id}')
async def delete_organization_(id: int, db: DBClient = Depends(acquire_db)):
    rowcount = await delete_organization(db, id)
    if rowcount == 0:
        raise HTTPException(status_code=404, detail="Organization not found")
    return rowcount
