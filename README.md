# pydantic-ai-kosa8

Run a [Pydantic AI](https://ai.pydantic.dev) agent's commands and file edits in a [kosa8 Cloud](https://kosa8.com) microVM sandbox instead of on your machine.

`Kosa8Sandbox` is a capability that supplies the run's workspace. Compose it with capabilities that use a workspace, such as `Coder`, `Shell` and `FileSystem` from [Pydantic AI Harness](https://ai.pydantic.dev/harness/), or your own tools through `ctx.workspace`.

## Install

```bash
pip install pydantic-ai-kosa8
```

Set `KOSA8_API_KEY` to a kosa8 Cloud API key (create one at [app.kosa8.com](https://app.kosa8.com)). kosa8 Cloud is a paid service, with plans from $29/month; there is no free tier.

## Quick start

```python
from pydantic_ai import Agent
from pydantic_ai_harness.coder import Coder
from pydantic_ai_kosa8 import Kosa8Sandbox

agent = Agent('anthropic:claude-sonnet-4-5', capabilities=[Kosa8Sandbox(ttl_s=3600), Coder()])
result = agent.run_sync('Clone https://github.com/pydantic/pydantic-ai and summarize how capabilities work.')
print(result.output)
```

The sandbox is created on the first workspace operation. Commands start in `/workspace` (set `working_dir=` to change it).

## Continue in the same sandbox

The sandbox's `WorkspaceRef` is in the message history and in `result.workspace.ref`. A later run with that history continues in the same sandbox, and kosa8 wakes it if it was asleep:

```python
again = agent.run_sync('Now run the tests.', message_history=result.all_messages())
```

Or pass the ref yourself: `agent.run_sync(..., workspace=Kosa8Sandbox().backend(ref))`.

## Clean up

Ending a run does not delete the sandbox. Delete it when you are done:

```python
await Kosa8Sandbox().destroy(result.workspace.ref)
```

or give new sandboxes a lifetime with `ttl_s` (60 s to 90 days), after which kosa8 deletes them, and `sleep_after_s` to put an idle one to sleep.

## Options

| Option | Default | |
|---|---|---|
| `api_key` | `KOSA8_API_KEY` | kosa8 Cloud API key |
| `base_url` | `KOSA8_CLOUD_URL` or `https://api.kosa8.com` | API address |
| `client` | one per run | an `AsyncKosa8Client` to share across runs (you close it) |
| `template` | account default | kosa8 template for a new sandbox |
| `region` | chosen by kosa8 | region for a new sandbox |
| `ttl_s` | none | lifetime of a new sandbox |
| `sleep_after_s` | account default | idle seconds before a new sandbox sleeps |
| `working_dir` | `/workspace` | where commands start; created if missing |
| `env` | none | environment variables for every command |

Errors from kosa8 Cloud are typed: `Kosa8PaymentRequiredError` (402, with `upgrade_url`), `Kosa8OrgFrozenError` (423), `Kosa8RateLimitError` (429) and `Kosa8Error` for the rest. A sandbox that was deleted raises Pydantic AI's `WorkspaceUnavailableError`.

## How it works

`Kosa8WorkspaceBackend` implements Pydantic AI's `WorkspaceBackend` and `SupportsCommands` protocols. Each command is one exec request to the kosa8 Cloud API, run by a small POSIX `sh` wrapper that:

- starts the command in a process group of its own, with stdin at EOF;
- returns as soon as the command exits, even if a background child keeps its output open;
- enforces `timeout=` inside the sandbox and kills the process group when it fires, or when the call is cancelled;
- returns stdout and stderr base64-encoded, so any bytes survive (undecodable bytes become U+FFFD), and reads large output back in chunks.

File operations (read, write, stat, list, remove, realpath) come from Pydantic AI's `Workspace`, which derives them through the sandbox's shell, so path handling and error types match Pydantic AI's other backends. The sandbox image needs `sh`, `base64`, `dd`, `find` and the other coreutils, which kosa8's templates include.

The backend passes Pydantic AI's `WorkspaceBackendSuite` conformance suite (39 of 40 rules run and pass against the bundled fake kosa8 API; the 40th is for the optional `SupportsRealpath` protocol, which this backend leaves to the shell).

## Security

Each kosa8 sandbox is a microVM. kosa8's SOC 2 audit is in progress; kosa8 does not have a SOC 2 report yet. See [kosa8.com/security](https://kosa8.com/security/).

## Development

```bash
uv venv && uv pip install -e . pytest ruff mypy
pytest                      # unit tests and the conformance suite, against a local fake API
ruff check . && mypy
```

The live tier runs the same conformance suite against kosa8 Cloud. It creates sandboxes (each with a 15-minute lifetime as a backstop) and deletes them:

```bash
PYDANTIC_AI_KOSA8_LIVE=1 KOSA8_API_KEY=... pytest -m kosa8_live
```

`pydantic_ai_kosa8/_kosa8.py` is the kosa8 Cloud client shared, unchanged, with [langchain-kosa8](https://github.com/kosa8ai/langchain-kosa8), inspect-kosa8 and harbor-kosa8.

## License

MIT
