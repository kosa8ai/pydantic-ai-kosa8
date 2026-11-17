"""Behaviour beyond the conformance suite: staging, large output, attach, the capability in an agent run."""

from __future__ import annotations

import pytest
from pydantic_ai import Agent, RunContext
from pydantic_ai.exceptions import UserError
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.workspaces import Workspace, WorkspaceOutputLimitError, WorkspaceRef, WorkspaceUnavailableError

from pydantic_ai_kosa8 import AsyncKosa8Client, Kosa8PaymentRequiredError, Kosa8Sandbox, Kosa8WorkspaceBackend

from .fake_kosa8 import FakeKosa8

pytestmark = pytest.mark.anyio


@pytest.fixture
def make(client: AsyncKosa8Client, dirs: tuple[str, str]):  # type: ignore[no-untyped-def]
    def make(ref: WorkspaceRef | None = None) -> Kosa8WorkspaceBackend:
        return Kosa8WorkspaceBackend(ref=ref, client=client, working_dir=dirs[0], run_dir=dirs[1])

    return make


async def test_no_io_until_first_operation(make, fake: FakeKosa8) -> None:  # type: ignore[no-untyped-def]
    backend = make()
    assert backend.ref is None and fake.requests == []
    await backend.working_dir()
    assert backend.ref is not None and backend.ref.provider == 'kosa8'
    assert [m for m, p in fake.requests if p == '/v1/sandboxes'] == ['POST']


@pytest.mark.parametrize('files_api', [True, False])
async def test_command_larger_than_one_request_is_staged(make, fake: FakeKosa8, files_api: bool, dirs: tuple[str, str]) -> None:  # type: ignore[no-untyped-def]
    fake.files_api = files_api
    backend = make()
    payload = 'x' * 150_000
    result = await backend.run(f'printf %s {payload} | wc -c', shell=True)
    assert (result.exit_code, result.stdout.strip()) == (0, '150000')
    # The staged script is removed after use.
    result = await backend.run(
        ['sh', '-c', 'ls -A "$1" "$2" 2>/dev/null | grep -c "\\.sh$" || true', 'sh', dirs[1], f'{dirs[0]}/.kosa8-run']
    )
    assert result.stdout.strip() == '0'


async def test_large_output_is_read_back_in_chunks(make) -> None:  # type: ignore[no-untyped-def]
    backend = make()
    size = 3 * 1024 * 1024 + 7
    result = await backend.run(['sh', '-c', f'head -c {size} /dev/zero | tr "\\0" a; printf err >&2'])
    assert (len(result.stdout), set(result.stdout), result.stderr) == (size, {'a'}, 'err')


async def test_output_over_the_cap_raises(make) -> None:  # type: ignore[no-untyped-def]
    backend = make()
    with pytest.raises(WorkspaceOutputLimitError) as info:
        await backend.run(['sh', '-c', f'head -c {11 * 1024 * 1024} /dev/zero'])
    assert len(info.value.stdout) == 10 * 1024 * 1024


async def test_env_layers_and_reserved_names(client: AsyncKosa8Client, dirs: tuple[str, str]) -> None:
    backend = Kosa8WorkspaceBackend(client=client, working_dir=dirs[0], run_dir=dirs[1], env={'A': '1', 'B': '1'})
    result = await backend.run(['sh', '-c', 'printf "%s%s%s" "$A" "$B" "${K8_RUN:-}"'], env={'B': '2'})
    assert result.stdout == '12'
    with pytest.raises(ValueError):
        await backend.run(['true'], env={'K8_CMD': 'x'})
    with pytest.raises(ValueError):
        await backend.run(['true'], env={'not a name': 'x'})


async def test_attach_wakes_a_sleeping_sandbox(make, fake: FakeKosa8) -> None:  # type: ignore[no-untyped-def]
    first = make()
    await first.working_dir()
    assert first.ref is not None
    fake.sandboxes[first.ref.id]['state'] = 'sleeping'
    attached = make(first.ref)
    assert (await attached.run(['true'])).exit_code == 0
    assert ('POST', f'/v1/sandboxes/{first.ref.id}/wake') in fake.requests


async def test_refs_are_validated(make) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(ValueError):
        make(WorkspaceRef(provider='e2b', id='abc'))
    with pytest.raises(WorkspaceUnavailableError):
        make(WorkspaceRef(provider='kosa8', id='../../etc'))
    with pytest.raises(WorkspaceUnavailableError):
        await make(WorkspaceRef(provider='kosa8', id='sbx_missing')).working_dir()


async def test_payment_required_propagates(fake: FakeKosa8, make) -> None:  # type: ignore[no-untyped-def]
    import httpx

    original = fake.handle

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.method == 'POST' and request.url.path == '/v1/sandboxes':
            return httpx.Response(402, json={'error': 'payment_required', 'message': 'add a card', 'upgrade_url': 'https://kosa8.com/x'})
        return await original(request)

    fake.handle = handle  # type: ignore[method-assign]
    with pytest.raises(Kosa8PaymentRequiredError) as info:
        await make().working_dir()
    assert info.value.upgrade_url == 'https://kosa8.com/x'


async def test_capability_validation() -> None:
    with pytest.raises(UserError):
        Kosa8Sandbox(working_dir='relative')
    with pytest.raises(UserError):
        Kosa8Sandbox(ttl_s=5)
    with pytest.raises(UserError):
        Kosa8Sandbox(defer_loading=True)
    with pytest.raises(ValueError):
        await Kosa8Sandbox().destroy(WorkspaceRef(provider='e2b', id='x'))


async def test_capability_in_an_agent_run(client: AsyncKosa8Client, fake: FakeKosa8, dirs: tuple[str, str]) -> None:
    calls = 0

    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            return ModelResponse(parts=[ToolCallPart('shell', {'command': 'echo hello > note.txt; pwd -P'})])
        return ModelResponse(parts=[TextPart('done')])

    sandbox = Kosa8Sandbox(client=client, working_dir=dirs[0])
    agent = Agent(FunctionModel(model), capabilities=[sandbox])

    @agent.tool
    async def shell(ctx: RunContext[None], command: str) -> str:
        result = await ctx.workspace.run(command, shell=True)
        return result.stdout

    result = await agent.run('write a note')
    ref = result.workspace.ref
    assert ref is not None and ref.provider == 'kosa8'
    assert await Workspace(sandbox.backend(ref)).read_text('note.txt') == 'hello\n'

    # A second run continues in the same sandbox from the history.
    calls = 0
    again = await agent.run('again', message_history=result.all_messages())
    assert again.workspace.ref == ref
    assert len([p for m, p in fake.requests if m == 'POST' and p == '/v1/sandboxes']) == 1

    await sandbox.destroy(ref)
    assert fake.sandboxes[ref.id]['state'] == 'deleted'
    await sandbox.destroy(ref)  # already gone: quiet
