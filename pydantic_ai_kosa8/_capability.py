"""Capability that supplies a kosa8 Cloud sandbox to an agent run."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.exceptions import UserError
from pydantic_ai.tools import AgentDepsT, RunContext
from pydantic_ai.workspaces import Workspace, WorkspaceBackend, WorkspaceRef, WrapperWorkspace

from ._backend import PROVIDER, Kosa8WorkspaceBackend, check_env, check_working_dir, destroy_sandbox
from ._kosa8 import AsyncKosa8Client

if TYPE_CHECKING:
    from pydantic_ai.agent import AgentRunResult
    from pydantic_ai.capabilities import WrapRunHandler


class _RunBackend(Kosa8WorkspaceBackend):
    """The backend `Kosa8Sandbox` built for one run; the capability closes its client when that run ends."""

    def __init__(self, *, run_id: str | None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.run_id = run_id


def _innermost(workspace: Workspace) -> WorkspaceBackend:
    current: WorkspaceBackend = workspace
    while isinstance(current, Workspace):
        current = current.wrapped if isinstance(current, WrapperWorkspace) else current.backend
    return current


@dataclass(kw_only=True)
class Kosa8Sandbox(AbstractCapability[AgentDepsT]):
    """Supply a [kosa8 Cloud](https://kosa8.com) microVM sandbox as the run's workspace.

    A run with no reference creates a sandbox on first use; pass a `WorkspaceRef` (from
    `result.workspace.ref` or the message history) to continue in an existing one. Ending a run
    does not delete the sandbox: call `destroy(ref)`, or set `ttl_s` so kosa8 deletes it.

    This capability supplies the workspace only. Compose it with capabilities that use it, such
    as the harness's `Coder`, `Shell` and `FileSystem`.
    """

    api_key: str | None = field(default=None, repr=False)
    """kosa8 Cloud API key; `KOSA8_API_KEY` when `None`."""

    base_url: str | None = None
    """API address; `KOSA8_CLOUD_URL` or `https://api.kosa8.com` when `None`."""

    client: AsyncKosa8Client | None = None
    """A client to share across runs on one event loop; you close it. When `None`, each run opens one."""

    template: str | None = None
    """kosa8 template a new sandbox starts from (see `GET /v1/templates`); the account default when `None`."""

    region: str | None = None
    """Region for a new sandbox; kosa8 picks one when `None`."""

    ttl_s: int | None = None
    """Lifetime of a new sandbox in seconds (60 s to 90 days), after which kosa8 deletes it."""

    sleep_after_s: int | None = None
    """Idle seconds before kosa8 puts a new sandbox to sleep; a command wakes it."""

    working_dir: str | None = None
    """Absolute directory commands start in and relative paths resolve against; `/workspace` when `None`.
    It is created if missing."""

    env: Mapping[str, str] | None = field(default=None, repr=False)
    """Environment variables every command gets; a command's `env` is layered on top."""

    def __post_init__(self) -> None:
        if self.defer_loading:
            raise UserError(
                '`Kosa8Sandbox` does not support `defer_loading=True`: the workspace is selected before deferred capabilities load.'
            )
        try:
            check_working_dir(self.working_dir)
            check_env(self.env)
        except (TypeError, ValueError) as error:
            raise UserError(str(error)) from error
        if self.ttl_s is not None and (type(self.ttl_s) is not int or not 60 <= self.ttl_s <= 90 * 86400):
            raise UserError(f'ttl_s must be a whole number of seconds from 60 to {90 * 86400}, got {self.ttl_s!r}.')
        if self.sleep_after_s is not None and (type(self.sleep_after_s) is not int or self.sleep_after_s < 0):
            raise UserError(f'sleep_after_s must be a whole number of seconds, got {self.sleep_after_s!r}.')

    @classmethod
    def from_spec(
        cls,
        *,
        id: str | None = None,
        description: str | None = None,
        defer_loading: bool = False,
        base_url: str | None = None,
        template: str | None = None,
        region: str | None = None,
        ttl_s: int | None = None,
        sleep_after_s: int | None = None,
        working_dir: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> Kosa8Sandbox[Any]:
        """Build from an agent spec. The API key is read from `KOSA8_API_KEY`, never from the spec."""
        return cls(
            id=id,
            description=description,
            defer_loading=defer_loading,
            base_url=base_url,
            template=template,
            region=region,
            ttl_s=ttl_s,
            sleep_after_s=sleep_after_s,
            working_dir=working_dir,
            env=env,
        )

    def _options(self) -> dict[str, Any]:
        return {
            'client': self.client,
            'api_key': self.api_key,
            'base_url': self.base_url,
            'template': self.template,
            'region': self.region,
            'ttl_s': self.ttl_s,
            'sleep_after_s': self.sleep_after_s,
            'working_dir': self.working_dir,
            'env': self.env,
        }

    def backend(self, ref: WorkspaceRef | None = None) -> Kosa8WorkspaceBackend:
        """A lazy backend for a new sandbox, or for the existing one `ref` names. Close it with `aclose()`."""
        return Kosa8WorkspaceBackend(ref=ref, **self._options())

    async def destroy(self, ref: WorkspaceRef) -> None:
        """Delete the sandbox `ref` names. A sandbox that no longer exists is already gone."""
        if ref.provider != PROVIDER:
            raise ValueError(f'unsupported workspace provider {ref.provider!r}; expected {PROVIDER!r}')
        if self.client is not None:
            await destroy_sandbox(self.client, ref.id)
            return
        async with AsyncKosa8Client(self.api_key, base_url=self.base_url) as client:
            await destroy_sandbox(client, ref.id)

    def get_workspace(self, ctx: RunContext[AgentDepsT], *, ref: WorkspaceRef | None) -> WorkspaceBackend | None:
        """The backend for this run. No I/O here: it attaches or creates on first use."""
        if ref is not None and ref.provider != PROVIDER:
            return None
        return _RunBackend(run_id=ctx.run_id, ref=ref, **self._options())

    async def wrap_run(self, ctx: RunContext[AgentDepsT], *, handler: WrapRunHandler) -> AgentRunResult[Any]:
        try:
            return await handler()
        finally:
            # Only the backend this run built: one passed in as `workspace=` belongs to its caller.
            backend = _innermost(ctx.workspace)
            if isinstance(backend, _RunBackend) and backend.run_id == ctx.run_id:
                # `result.workspace` outlives the run; a later operation on it reopens a client.
                await backend.aclose()
