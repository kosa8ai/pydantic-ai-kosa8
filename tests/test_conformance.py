"""Pydantic AI's workspace backend conformance suite, run against `Kosa8WorkspaceBackend`.

`TestFakeKosa8Backend` runs on POSIX hosts over the fake kosa8 API, where commands run on the
host. `TestLiveKosa8Backend` runs the same rules against kosa8 Cloud and is gated in conftest.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
from pydantic_ai.workspaces import WorkspaceBackend, WorkspaceRef
from pydantic_ai.workspaces.conformance import WorkspaceBackendSuite

from pydantic_ai_kosa8 import AsyncKosa8Client, Kosa8Sandbox, Kosa8WorkspaceBackend

from .conftest import LIVE_TTL_S


@pytest.mark.skipif(os.name != 'posix', reason='needs a POSIX host')
class TestFakeKosa8Backend(WorkspaceBackendSuite):
    @pytest.fixture
    def make(self, client: AsyncKosa8Client, dirs: tuple[str, str]) -> Callable[..., Kosa8WorkspaceBackend]:
        def make(ref: WorkspaceRef | None = None) -> Kosa8WorkspaceBackend:
            return Kosa8WorkspaceBackend(ref=ref, client=client, working_dir=dirs[0], run_dir=dirs[1])

        return make

    @pytest.fixture
    def backend(self, make: Callable[..., Kosa8WorkspaceBackend]) -> Kosa8WorkspaceBackend:
        return make()

    @pytest.fixture
    def fresh_backend(self, make: Callable[..., Kosa8WorkspaceBackend]) -> Callable[[], WorkspaceBackend]:
        return make

    @pytest.fixture
    def attach_backend(self, make: Callable[..., Kosa8WorkspaceBackend]) -> Callable[[WorkspaceRef], WorkspaceBackend]:
        return lambda ref: make(ref)

    @pytest.fixture
    def destroy_environment(self, client: AsyncKosa8Client) -> Callable[[WorkspaceBackend], Awaitable[None]]:
        async def destroy(backend: WorkspaceBackend) -> None:
            assert backend.ref is not None
            await Kosa8Sandbox(client=client).destroy(backend.ref)

        return destroy


@pytest.mark.kosa8_live
class TestLiveKosa8Backend(WorkspaceBackendSuite):  # pragma: no cover - live tier
    """Each rule gets its own sandbox (deleted afterwards), with a short lifetime as a backstop."""

    @pytest.fixture
    async def created(self) -> AsyncIterator[list[Kosa8WorkspaceBackend]]:
        backends: list[Kosa8WorkspaceBackend] = []
        yield backends
        for backend in backends:
            try:
                if backend.ref is not None:
                    await Kosa8Sandbox().destroy(backend.ref)
            finally:
                await backend.aclose()

    @pytest.fixture
    def fresh_backend(self, created: list[Kosa8WorkspaceBackend]) -> Callable[[], WorkspaceBackend]:
        def make() -> WorkspaceBackend:
            backend = Kosa8WorkspaceBackend(ttl_s=LIVE_TTL_S)
            created.append(backend)
            return backend

        return make

    @pytest.fixture
    def backend(self, fresh_backend: Callable[[], WorkspaceBackend]) -> WorkspaceBackend:
        return fresh_backend()

    @pytest.fixture
    async def attach_backend(self) -> AsyncIterator[Callable[[WorkspaceRef], WorkspaceBackend]]:
        attached: list[Kosa8WorkspaceBackend] = []

        def attach(ref: WorkspaceRef) -> WorkspaceBackend:
            backend = Kosa8WorkspaceBackend(ref=ref)
            attached.append(backend)
            return backend

        yield attach
        for backend in attached:
            await backend.aclose()

    @pytest.fixture
    def destroy_environment(self) -> Callable[[WorkspaceBackend], Awaitable[None]]:
        async def destroy(backend: WorkspaceBackend) -> None:
            assert backend.ref is not None
            await Kosa8Sandbox().destroy(backend.ref)

        return destroy
