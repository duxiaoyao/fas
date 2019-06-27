import hashlib
from pathlib import Path
from typing import Dict

from fas.environment import ENV

SCRIPT_DIR = ENV.root_dir / 'db'


def load_versions(*, after: int = 0) -> Dict[int, Path]:
    versions = {}
    expected_version = None
    for sql_path in sorted(SCRIPT_DIR.rglob('*.sql'), key=lambda p: p.stem, reverse=True):
        relative_path = sql_path.relative_to(SCRIPT_DIR)
        if '-' not in sql_path.stem:
            raise Exception(f'Invalid migration script {relative_path}')
        version = int(sql_path.stem.split('-', maxsplit=1)[0])
        if version <= after:
            break
        if version in versions:
            raise Exception(f'Script {relative_path} duplicated with {versions[version].relative_to(SCRIPT_DIR)}')
        if expected_version is not None and expected_version != version:
            raise Exception(f'Script versions should be continuous numbers: missed version {expected_version}')
        else:
            expected_version = version - 1
        versions[version] = sql_path
    if expected_version is not None and expected_version != after:
        raise Exception(f'Script versions should be continuous numbers: missed version {expected_version}')
    return versions


def lock_scripts():
    locked_count = 0
    expected_version = 1
    for sql_path in sorted(SCRIPT_DIR.rglob('*.sql'), key=lambda p: p.stem):
        relative_path = sql_path.relative_to(SCRIPT_DIR)
        if '-' not in sql_path.stem:
            raise Exception(f'Invalid migration script {relative_path}')
        version = int(sql_path.stem.split('-', maxsplit=1)[0])
        if version != expected_version:
            raise Exception(f'Script versions should be continuous numbers: missed version {expected_version}')
        expected_version += 1

        actual_md5 = calculate_md5_hash(sql_path)
        lock_path = sql_path.with_suffix('.locked')
        if lock_path.exists():
            expected_md5 = lock_path.read_text()
            if actual_md5 != expected_md5:
                raise Exception(f'Found locked-then-changed migration script {relative_path}')
        else:
            lock_path.write_text(actual_md5)
            locked_count += 1
    return locked_count


def check_no_locked_scripts_changed():
    for lock_path in sorted(SCRIPT_DIR.rglob('*.locked'), key=lambda p: p.stem, reverse=True):
        sql_path = lock_path.with_suffix('.sql')
        relative_sql_path = sql_path.relative_to(SCRIPT_DIR)
        if not sql_path.exists():
            raise Exception(f'Found locked-then-removed migration script {relative_sql_path}')
        expected_md5 = lock_path.read_text()
        actual_md5 = calculate_md5_hash(sql_path)
        if actual_md5 != expected_md5:
            raise Exception(f'Found locked-then-changed migration script {relative_sql_path}')


def check_no_scripts_not_locked():
    for sql_path in sorted(SCRIPT_DIR.rglob('*.sql'), key=lambda p: p.stem, reverse=True):
        lock_path = sql_path.with_suffix('.locked')
        if not lock_path.exists():
            raise Exception(f'Found not-locked migration script {sql_path.relative_to(SCRIPT_DIR)}')


def calculate_md5_hash(path: Path) -> str:
    m = hashlib.md5()
    m.update(path.read_bytes())
    return m.hexdigest()
