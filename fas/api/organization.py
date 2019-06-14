from typing import List

from fastapi import APIRouter, Body, HTTPException
from starlette.requests import Request
from starlette.status import HTTP_201_CREATED, HTTP_204_NO_CONTENT

from fas.model.organization import *
from .models import ResourceID, Message

router = APIRouter()


@router.get('/', response_model=List[Organization])
async def list_organizations_(request: Request):
    return await list_organizations(request.state.db)


@router.post('/', response_model=ResourceID, status_code=HTTP_201_CREATED)
async def create_organization_(request: Request, name: str = Body(..., min_length=2, max_length=32)):
    id = await create_organization(request.state.db, name)
    return ResourceID(id=id)


@router.get('/{id}', response_model=Organization, responses={404: {"model": Message}})
async def get_organization_(request: Request, id: int):
    org = await get_organization(request.state.db, id)
    if not org:
        raise HTTPException(status_code=404, detail='Organization not exist')
    return org


@router.put('/{id}', status_code=HTTP_204_NO_CONTENT, responses={404: {"model": Message}})
async def update_organization_(request: Request, id: int, name: str = Body(..., min_length=2, max_length=32)):
    rowcount = await update_organization(request.state.db, id, name)
    if rowcount == 0:
        raise HTTPException(status_code=404, detail='Organization not exist')


@router.delete('/{id}', status_code=HTTP_204_NO_CONTENT, responses={404: {"model": Message}})
async def delete_organization_(request: Request, id: int):
    rowcount = await delete_organization(request.state.db, id)
    if rowcount == 0:
        raise HTTPException(status_code=404, detail='Organization not exist')
