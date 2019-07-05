import logging
from typing import List

from fastapi import APIRouter, Body, HTTPException
from starlette.requests import Request

from fas.model.organization import *
from fas.util.database import UniqueViolationError
from fas.util.model import ResourceID, Message

LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.get('/', response_model=List[Organization])
async def list(request: Request):
    return await list_organizations(request.state.db)


@router.post('/', response_model=ResourceID, status_code=201, responses={409: {'model': Message}})
async def create(request: Request, name: str = Body(..., min_length=2, max_length=32)):
    try:
        org = await create_organization(request.state.db, name)
        return ResourceID(id=org.id)
    except UniqueViolationError:
        err_msg = f'name `{name}` is already used'
        LOGGER.exception(f'Cannot create organization: {err_msg}')
        raise HTTPException(status_code=409, detail=err_msg)


@router.get('/{id}', response_model=Organization, responses={404: {'model': Message}})
async def get(request: Request, id: int):
    org = await get_organization(request.state.db, id)
    if not org:
        raise HTTPException(status_code=404, detail=f'Organization #{id} not found')
    return org


@router.put('/{id}', status_code=204, responses={409: {'model': Message}, 404: {'model': Message}})
async def update(request: Request, id: int, name: str = Body(..., min_length=2, max_length=32)):
    try:
        rowcount = await update_organization(request.state.db, id, name)
    except UniqueViolationError:
        err_msg = f'name `{name}` is already used'
        LOGGER.exception(f'Cannot update organization #{id}: {err_msg}')
        raise HTTPException(status_code=409, detail=err_msg)
    if rowcount == 0:
        raise HTTPException(status_code=404, detail=f'Organization #{id} not found')


@router.delete('/{id}', status_code=204, responses={404: {'model': Message}})
async def delete(request: Request, id: int):
    rowcount = await delete_organization(request.state.db, id)
    if rowcount == 0:
        raise HTTPException(status_code=404, detail=f'Organization #{id} not found')
