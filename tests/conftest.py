import pytest
from dynaconf import settings


@pytest.fixture(scope='session', autouse=True)
def print_current_env():
    print(f'Current ENV: {settings.ENV_FOR_DYNACONF}')
