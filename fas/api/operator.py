import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, params
from pydantic import BaseModel, Schema
from starlette.requests import Request
from starlette.responses import Response

from fas.model.operator import get_operator_by_mobile, Operator, get_operator_by_id
from fas.util.model import Message
from fas.util.web import get_secure_cookie, set_secure_cookie, delete_cookie, verify_password

LOGGER = logging.getLogger(__name__)

router = APIRouter()

OPERATOR_COOKIE_NAME = 'op'


class OperatorLogin(BaseModel):
    mobile: str = Schema(..., regex=r'^1\d{10}$')
    password: str = Schema(..., min_length=8)


@router.post('/login', status_code=204, responses={422: {'model': Message}})
async def login(request: Request, response: Response, organization_id: int, ol: OperatorLogin):
    operator = await get_operator_by_mobile(request.state.db, organization_id, ol.mobile)
    err_reason = None
    if not operator or not verify_password(operator.password_hash, ol.password):
        err_reason = '手机号或密码不对'
    elif not operator.active:
        err_reason = '账号被禁用'
    if err_reason:
        raise HTTPException(status_code=422, detail=err_reason)
    LOGGER.info(f'Operator {ol.mobile}@{organization_id} logged in')
    set_secure_cookie(response, OPERATOR_COOKIE_NAME, str(operator.id), path='/', expires_days=None)


@router.get('/logout', status_code=204)
def logout(response: Response):
    delete_cookie(response, OPERATOR_COOKIE_NAME)


@router.get('/current')
async def get_current_operator_(request: Request):
    return {'operator': await get_current_operator(request)}


def get_current_operator_id(request: Request) -> Optional[int]:
    operator_id = get_secure_cookie(request, OPERATOR_COOKIE_NAME, max_age_days=None)
    try:
        return int(operator_id)
    except (TypeError, ValueError):
        return None


async def get_current_operator(request: Request) -> Optional[Operator]:
    operator_id = get_current_operator_id(request)
    if not operator_id:
        return None
    return await get_operator_by_id(request.state.db, operator_id)


def require_auth(*, is_admin: bool = False) -> params.Depends:
    return Depends(AuthRequiredValidator(is_admin=is_admin))


class AuthRequiredValidator:
    __slots__ = ('is_admin',)

    def __init__(self, *, is_admin: bool = False) -> None:
        self.is_admin: bool = is_admin

    async def __call__(self, request: Request) -> None:
        if not hasattr(request.state, 'operator'):
            request.state.operator = await get_current_operator(request)
        if not request.state.operator:
            raise HTTPException(status_code=401, detail='Not logged in as an operator',
                                headers={'WWW-Authenticate': '/operators/login'})
        if self.is_admin and not request.state.operator.is_admin:
            raise HTTPException(status_code=403, detail='Permission denied: admin operator required')
