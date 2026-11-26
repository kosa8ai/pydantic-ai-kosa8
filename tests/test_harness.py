"""An Agent run where Pydantic AI Harness's `Shell` and `FileSystem` tools run in the kosa8 sandbox.

`Kosa8Sandbox` supplies the run's workspace; `Shell` and `FileSystem` know nothing about kosa8. The model is a
`FunctionModel`, so the run is deterministic, and the kosa8 API is the bundled fake, so every operation can be
checked on the API side as well as in the tool results.
"""

from __future__ import annotations

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.workspaces import Workspace
from pydantic_ai.workspaces.workspace import workspace_layers
from pydantic_ai_harness import FileSystem, Shell

from pydantic_ai_kosa8 import AsyncKosa8Client, Kosa8Sandbox, Kosa8WorkspaceBackend

from .fake_kosa8 import FakeKosa8

pytestmark = pytest.mark.anyio

STEPS: list[tuple[str, dict[str, object]]] = [
    ('write_file', {'path': 'app.sh', 'content': 'echo hello from kosa8\n'}),
    ('run_command', {'command': 'sh app.sh > out.txt && cat out.txt && pwd -P'}),
    ('read_file', {'path': 'out.txt'}),
]


def _scripted(tool_names: list[str]):  # type: ignore[no-untyped-def]
    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tool_names[:] = sorted(t.name for t in info.function_tools)
        done = sum(1 for m in messages if isinstance(m, ModelRequest) for p in m.parts if isinstance(p, ToolReturnPart))
        if done < len(STEPS):
            name, args = STEPS[done]
            return ModelResponse(parts=[ToolCallPart(name, args)])
        return ModelResponse(parts=[TextPart('done')])

    return model


def _returns(messages: list[ModelMessage]) -> dict[str, str]:
    return {p.tool_name: str(p.content) for m in messages if isinstance(m, ModelRequest) for p in m.parts if isinstance(p, ToolReturnPart)}


async def test_shell_and_file_system_tools_run_in_the_kosa8_sandbox(
    client: AsyncKosa8Client, fake: FakeKosa8, dirs: tuple[str, str]
) -> None:
    tool_names: list[str] = []
    sandbox = Kosa8Sandbox(client=client, working_dir=dirs[0])
    agent = Agent(FunctionModel(_scripted(tool_names)), capabilities=[sandbox, FileSystem(), Shell()])

    result = await agent.run('write a script, run it, read its output')

    assert result.output == 'done'
    assert {'read_file', 'run_command', 'write_file'} <= set(tool_names)

    # The run's workspace is the kosa8 backend, selected by the capability.
    assert issubclass(workspace_layers(result.workspace)[-1], Kosa8WorkspaceBackend)
    ref = result.workspace.ref
    assert ref is not None and ref.provider == 'kosa8' and ref.id in fake.sandboxes

    # Each tool reached the sandbox through the kosa8 API: one sandbox created, commands sent to its exec endpoint.
    assert [p for m, p in fake.requests if m == 'POST' and p == '/v1/sandboxes'] == ['/v1/sandboxes']
    assert any(m == 'POST' and p.startswith(f'/v1/sandboxes/{ref.id}/exec') for m, p in fake.requests)

    returns = _returns(result.all_messages())
    assert 'hello from kosa8' in returns['run_command']
    assert dirs[0] in returns['run_command']  # the command started in the sandbox's working dir
    assert 'hello from kosa8' in returns['read_file']

    # The file FileSystem wrote is in the sandbox, readable through a fresh backend attached by ref.
    workspace = Workspace(sandbox.backend(ref))
    assert await workspace.read_text('app.sh') == 'echo hello from kosa8\n'
    assert await workspace.read_text('out.txt') == 'hello from kosa8\n'

    await sandbox.destroy(ref)
    assert fake.sandboxes[ref.id]['state'] == 'deleted'


@pytest.mark.kosa8_live
async def test_shell_and_file_system_tools_live() -> None:  # pragma: no cover - live job
    sandbox = Kosa8Sandbox(ttl_s=900)
    agent = Agent(FunctionModel(_scripted([])), capabilities=[sandbox, FileSystem(), Shell()])
    result = await agent.run('write a script, run it, read its output')
    ref = result.workspace.ref
    assert ref is not None and ref.provider == 'kosa8'
    try:
        returns = _returns(result.all_messages())
        assert 'hello from kosa8' in returns['run_command']
        assert '/workspace' in returns['run_command']
        assert 'hello from kosa8' in returns['read_file']
    finally:
        await sandbox.destroy(ref)
