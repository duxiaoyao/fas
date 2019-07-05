import asyncio

from blessings import Terminal
from dynaconf import settings
from invoke import Collection, task

from fas.model.organization import add_organization
from fas.util.console import confirm
from fas.util.database import DBPool
from fas.util.web import generate_password

t = Terminal()


@task
def add_org(c, name, admin_name, admin_mobile):
    """
    Add an organization with the 1st admin operator
    :param name: organization name
    :param admin_name: admin operator's name
    :param admin_mobile: admin operator's mobile
    """
    if not confirm(f'Add organization {name} with the 1st admin operator {admin_name} - {admin_mobile} ?',
                   assume_yes=False):
        print(t.yellow(f'Aborted adding organization {name}'))
        return
    admin_password = generate_password(size=8)
    org, admin = asyncio.run(_add_org(name, admin_name, admin_mobile, admin_password))
    print(t.green(f'Added organization: id={org.id}, name={org.name}, admin={admin.name}'))
    print(t.red(f'Please inform {admin.name} to log on with mobile {admin.mobile} and password {admin_password}'))


async def _add_org(name: str, admin_name: str, admin_mobile: str, admin_password: str):
    async with DBPool(**settings.DB) as pool:
        async with pool.acquire() as db:
            return await add_organization(db, name, admin_name, admin_mobile, admin_password)


op_tasks = Collection('op', add_org)
