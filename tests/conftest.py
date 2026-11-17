"""Shared fixtures: the fake kosa8 API, and the gate for the live tier."""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from pydantic_ai_kosa8 import AsyncKosa8Client

from .fake_kosa8 import FakeKosa8

LIVE_TTL_S = 900


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Run the live tier only with `PYDANTIC_AI_KOSA8_LIVE=1` and a non-empty `KOSA8_API_KEY`."""
    if item.get_closest_marker('kosa8_live') is None:
        return
    if os.getenv('PYDANTIC_AI_KOSA8_LIVE') == '1' and os.getenv('KOSA8_API_KEY'):  # pragma: no cover - live job
        return
    pytest.skip('requires PYDANTIC_AI_KOSA8_LIVE=1 and a non-empty KOSA8_API_KEY')


@pytest.fixture
def anyio_backend() -> str:
    return 'asyncio'


@pytest.fixture
def dirs(tmp_path: Path) -> tuple[str, str]:
    """(working dir, run dir) on the host, resolved so `pwd -P` matches them."""
    work, run = tmp_path / 'work', tmp_path / 'run'
    work.mkdir()
    run.mkdir()
    return str(work.resolve()), str(run.resolve())


# util-linux `setsid`, for hosts without one (macOS): a new session, forking first if already a group leader.
_SETSID = """#!{python}
import os, sys
try:
    os.setsid()
except OSError:
    if os.fork():
        os._exit(0)
    os.setsid()
os.execvp(sys.argv[1], sys.argv[1:])
"""


@pytest.fixture(scope='session')
def setsid_dir(tmp_path_factory: pytest.TempPathFactory) -> str | None:
    if shutil.which('setsid'):
        return None
    bin_dir = tmp_path_factory.mktemp('bin')
    shim = bin_dir / 'setsid'
    shim.write_text(_SETSID.format(python=sys.executable))
    shim.chmod(0o755)
    return str(bin_dir)


@pytest.fixture
async def fake(setsid_dir: str | None) -> AsyncIterator[FakeKosa8]:
    fake = FakeKosa8(path_prefix=setsid_dir)
    yield fake
    await fake.reap()


@pytest.fixture
async def client(fake: FakeKosa8, dirs: tuple[str, str]) -> AsyncIterator[AsyncKosa8Client]:
    fake.files_root = dirs[0]
    client = AsyncKosa8Client(fake.api_key, base_url='http://localhost', http_client=fake.async_client(), files_root=dirs[0])
    yield client
    await client._http.aclose()
