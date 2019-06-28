import logging

from fastapi import APIRouter, Body
from starlette.requests import Request
from starlette.responses import Response

from fas.util.web import get_secure_cookie, set_secure_cookie

LOGGER = logging.getLogger(__name__)

router = APIRouter()


@router.post('/login')
def login_(request: Request, response: Response, name: str = Body(..., min_length=2, max_length=32)):
    session_id = get_secure_cookie(request, 'sid')
    if session_id:
        status = f'already logged in as {session_id}'
    else:
        set_secure_cookie(response, 'sid', name, path='/')
        status = f'just logged in as {name}'
    return {'name': name, 'status': status}


@router.get('/status')
def get_login_status(request: Request):
    session_id = get_secure_cookie(request, 'sid')
    return {'session_id': session_id}
