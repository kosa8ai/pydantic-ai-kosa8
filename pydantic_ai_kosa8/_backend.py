"""A kosa8 Cloud sandbox behind Pydantic AI's `WorkspaceBackend` protocol.

The backend implements `SupportsCommands`; `Workspace` derives the file operations
(read, write, stat, list, realpath, ...) through the sandbox's shell, so path checks and
error types follow Pydantic AI's own rules.

How a command runs (buffered exec requests to the kosa8 Cloud API):

- Every request is a buffered exec. kosa8's streamed exec can run in a PID namespace and `/tmp`
  of its own, where a later request cannot see or stop the processes it started.
- One request starts the wrapper below in the background and returns; further requests wait for
  it, each for at most `_WAIT_S` seconds, so a command may run longer than one buffered exec.

- A POSIX `sh` wrapper starts the command in a process group of its own (`setsid`, or job
  control where `setsid` is missing), with stdin at EOF and stdout/stderr redirected to files in
  a per-command run directory. It waits for the direct command only, so a background child that
  keeps its output open does not hold the call up.
- The wrapper then prints a status line and the two outputs base64-encoded, so any bytes survive
  the JSON transport; undecodable bytes become U+FFFD here. Output too large for one answer is
  read back in chunks by further execs.
- A `timeout=` is enforced inside the sandbox by a watchdog that kills the command's process
  group. Cancelling the call sends a second exec that kills it.
- A command too large for one request body is staged in a file first (through the files API
  when the sandbox offers it, else through exec).
"""

from __future__ import annotations

import base64
import json
import math
import posixpath
import re
import shlex
import uuid
from collections.abc import Mapping, Sequence

import anyio
from pydantic_ai.workspaces import (
    CommandResult,
    WorkspaceCommand,
    WorkspaceError,
    WorkspaceOutputLimitError,
    WorkspaceRef,
    WorkspaceTimeoutError,
    WorkspaceUnavailableError,
)

from ._kosa8 import (
    AsyncKosa8Client,
    ExecOutput,
    ExecRequest,
    Kosa8Error,
    Kosa8TimeoutError,
)

PROVIDER = 'kosa8'
DEFAULT_WORKING_DIR = '/workspace'

# The API takes a 64 KiB JSON body per exec; past this the command is staged in a file.
_BODY_LIMIT = 56_000
# Raw bytes the wrapper may print inline (base64 grows them by a third; a buffered exec keeps 1 MiB per stream).
_INLINE_BYTES = 512 * 1024
_CHUNK_BYTES = 384 * 1024
# Same cap as Pydantic AI's local backend.
_MAX_CAPTURE_BYTES = 10 * 1024 * 1024
# Raw bytes of a staged command per exec, when the files API is not available.
_STAGE_CHUNK_BYTES = 30_000
_SERVER_GRACE_S = 15
# Seconds one wait request polls for the wrapper to finish (under the buffered exec limit).
_WAIT_S = 50
_KILL_TIMEOUT_S = 15
_GONE_CHECKS = 6
_GONE_CHECK_INTERVAL_S = 0.5

_SANDBOX_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$')
_ENV_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_GONE_STATES = frozenset({'deleted', 'expired', 'failed', 'error'})
_ASLEEP_STATES = frozenset({'sleeping', 'paused', 'stopped'})

# $K8_RUN run dir, $K8_CMD (or the script file $K8_CMDFILE), $K8_CWD, $K8_TIMEOUT (whole seconds), $K8_INLINE.
# Prints "K8RUN <exit> <timed out> <stdout bytes> <stderr bytes>", then, when they fit inline, the two
# outputs in base64 separated by a K8SEP line. The command never sees the K8_* variables.
_RUN = (
    'r=$K8_RUN; c=$K8_CMD; cf=$K8_CMDFILE; w=$K8_CWD; t=$K8_TIMEOUT; n=$K8_INLINE; '
    'unset K8_RUN K8_CMD K8_CMDFILE K8_CWD K8_TIMEOUT K8_INLINE; '
    'mkdir -p "$r" 2>/dev/null || { echo "K8FAIL cannot create $r"; exit 0; }; '
    'cd "$w" 2>/dev/null || { echo "K8FAIL cannot change to $w"; exit 0; }; '
    'if [ -n "$cf" ]; then set -- "$cf"; else set -- -c "$c"; fi; '
    'if command -v setsid >/dev/null 2>&1; then setsid sh "$@" </dev/null >"$r/o" 2>"$r/e" & '
    'else set -m 2>/dev/null; sh "$@" </dev/null >"$r/o" 2>"$r/e" & fi; '
    'p=$!; echo "$p" > "$r/pid"; '
    'k8k(){ kill -KILL -- -$p 2>/dev/null || kill -KILL $p 2>/dev/null; }; '
    "trap 'k8k' TERM HUP INT; "
    '[ -f "$r/cancel" ] && k8k; '
    'wd=; if [ -n "$t" ]; then ( sleep "$t"; : > "$r/to"; k8k ) </dev/null >/dev/null 2>&1 & wd=$!; fi; '
    'wait "$p"; rc=$?; '
    'while kill -0 "$p" 2>/dev/null && [ ! -f "$r/to" ] && [ ! -f "$r/cancel" ]; do wait "$p"; rc=$?; done; '
    '[ -n "$wd" ] && kill "$wd" 2>/dev/null; '
    '[ -n "$cf" ] && rm -f "$cf"; '
    'to=0; [ -f "$r/to" ] && to=1; '
    'os=$(wc -c < "$r/o"); es=$(wc -c < "$r/e"); os=$((os + 0)); es=$((es + 0)); '
    'echo "K8RUN $rc $to $os $es"; '
    'if [ $((os + es)) -le "$n" ]; then base64 < "$r/o"; echo K8SEP; base64 < "$r/e"; rm -rf "$r"; fi'
)

# $1 the wrapper script, $2 run dir: start the wrapper detached; it writes its answer to $2.out, then $2.done.
_LAUNCH = (
    'r=$2; mkdir -p "${r%/*}" 2>/dev/null; rm -f "$r.out" "$r.done"; '
    'if command -v setsid >/dev/null 2>&1; then '
    'setsid sh -c \'sh -c "$1" k8 >"$2.out" 2>/dev/null; : >"$2.done"\' k8 "$1" "$r" </dev/null >/dev/null 2>&1 & '
    'else ( sh -c "$1" k8 >"$r.out" 2>/dev/null; : >"$r.done" ) </dev/null >/dev/null 2>&1 & fi; '
    'echo K8STARTED'
)

# $1 run dir, $2 tenths of a second to wait: print the wrapper's answer once it is done, else K8WAIT.
_WAIT = (
    'r=$1; i=0; while [ ! -f "$r.done" ] && [ "$i" -lt "$2" ]; do sleep 0.1; i=$((i + 1)); done; '
    'if [ -f "$r.done" ]; then cat "$r.out"; rm -f "$r.out" "$r.done"; else echo K8WAIT; fi'
)

# $1 run dir: mark it cancelled and kill the command's process group, if it started.
_KILL = (
    'r=$1; mkdir -p "$r" 2>/dev/null; : > "$r/cancel"; '
    'p=$(cat "$r/pid" 2>/dev/null) && [ -n "$p" ] && { kill -KILL -- -"$p" 2>/dev/null || kill -KILL "$p" 2>/dev/null; }; '
    'exit 0'
)

# $1 file, $2 chunk size, $3 chunk index: one chunk in base64.
_CHUNK = 'dd if="$1" bs="$2" skip="$3" count=1 2>/dev/null | base64'

# $1 file, $2 w or a; data in K8_DATA (base64).
_STAGE = (
    'd=$K8_DATA; unset K8_DATA; mkdir -p "${1%/*}" || exit 1; '
    'if [ "$2" = w ]; then : > "$1" || exit 1; fi; printf %s "$d" | base64 -d >> "$1"'
)

# $1 directory: create it and print its canonical path.
_WORKING_DIR = 'mkdir -p "$1" && cd "$1" && pwd -P'


def check_sandbox_id(sandbox_id: str) -> str:
    """Validate a sandbox id taken from a ref, which can come from stored or untrusted history."""
    if not isinstance(sandbox_id, str) or not _SANDBOX_ID_RE.match(sandbox_id):
        raise WorkspaceUnavailableError(f'not a kosa8 sandbox id: {sandbox_id!r}')
    return sandbox_id


def check_env(env: Mapping[str, str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in (env or {}).items():
        if not isinstance(key, str) or not _ENV_NAME_RE.match(key):
            raise ValueError(f'invalid environment variable name: {key!r}')
        if key.startswith('K8_'):
            raise ValueError(f'environment variable names starting with K8_ are reserved: {key!r}')
        if not isinstance(value, str):
            raise TypeError(f'environment variable {key!r} must be a string, got {type(value).__name__}')
        out[key] = value
    return out


def check_working_dir(value: str | None) -> None:
    if value is not None and (not isinstance(value, str) or not value.startswith('/')):
        raise ValueError(f'working_dir must be an absolute path, got {value!r}')


def _is_gone(error: Kosa8Error) -> bool:
    return error.status in (404, 410)


def _decode(text: str) -> bytes:
    cleaned = re.sub(r'[^A-Za-z0-9+/=]', '', text)
    return base64.b64decode(cleaned) if cleaned else b''


class Kosa8WorkspaceBackend:
    """A kosa8 Cloud sandbox as a Pydantic AI workspace backend.

    Built without I/O. The first operation creates a sandbox (or attaches to the one `ref` names,
    waking it if it sleeps) and records its ref; the backend never deletes it. Use
    `Kosa8Sandbox.destroy(ref)`, or give the sandbox a lifetime with `ttl_s`.
    """

    def __init__(
        self,
        *,
        ref: WorkspaceRef | None = None,
        client: AsyncKosa8Client | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        template: str | None = None,
        region: str | None = None,
        ttl_s: int | None = None,
        sleep_after_s: int | None = None,
        working_dir: str | None = None,
        env: Mapping[str, str] | None = None,
        run_dir: str = '/tmp/.kosa8-pydantic-ai',
    ) -> None:
        if ref is not None:
            if ref.provider != PROVIDER:
                raise ValueError(f'unsupported workspace provider {ref.provider!r}; expected {PROVIDER!r}')
            check_sandbox_id(ref.id)
        check_working_dir(working_dir)
        self._ref = ref
        self._client = client
        self._own_client: AsyncKosa8Client | None = None
        self._api_key = api_key
        self._base_url = base_url
        self._template = template
        self._region = region
        self._ttl_s = ttl_s
        self._sleep_after_s = sleep_after_s
        self._configured_dir = working_dir or DEFAULT_WORKING_DIR
        self._env = check_env(env)
        self._run_root = run_dir.rstrip('/') or '/tmp'
        self._working_dir: str | None = None
        self._lock = anyio.Lock()
        self._files_api: bool | None = None

    def __repr__(self) -> str:
        return f'Kosa8WorkspaceBackend(ref={self._ref!r})'

    # Identity ----------------------------------------------------------------

    @property
    def ref(self) -> WorkspaceRef | None:
        return self._ref

    @property
    def sandbox_id(self) -> str | None:
        return None if self._ref is None else self._ref.id

    async def working_dir(self) -> str:
        await self._connect()
        assert self._working_dir is not None
        return self._working_dir

    # Client ------------------------------------------------------------------

    def client(self) -> AsyncKosa8Client:
        """The kosa8 client this backend uses: the one passed in, or its own (opened on demand)."""
        if self._client is not None:
            return self._client
        if self._own_client is None:
            self._own_client = AsyncKosa8Client(self._api_key, base_url=self._base_url, stream=False)
        return self._own_client

    async def aclose(self) -> None:
        """Close the client this backend opened. The sandbox keeps running; a later operation reopens one."""
        own, self._own_client = self._own_client, None
        if own is not None:
            await own.aclose()

    # Connecting --------------------------------------------------------------

    async def _connect(self) -> str:
        if self._working_dir is not None:
            assert self._ref is not None
            return self._ref.id
        async with self._lock:  # concurrent first operations share one sandbox
            if self._working_dir is None:
                client = self.client()
                if self._ref is None:
                    box = await client.create_sandbox(
                        template=self._template, region=self._region, ttl_s=self._ttl_s, sleep_after_s=self._sleep_after_s
                    )
                    self._ref = WorkspaceRef(provider=PROVIDER, id=check_sandbox_id(box['id']))
                else:
                    await self._attach(client, self._ref.id)
                out = await self._exec(ExecRequest(argv=['/bin/sh', '-c', _WORKING_DIR, 'k8', self._configured_dir]))
                path = out.stdout.strip()
                if out.exit_code != 0 or not path.startswith('/'):
                    raise WorkspaceError(
                        f'could not create the working directory {self._configured_dir!r} in kosa8 sandbox '
                        f'{self._ref.id}: {(out.stderr or out.stdout).strip()[:300]}'
                    )
                self._working_dir = path
        assert self._ref is not None
        return self._ref.id

    async def _attach(self, client: AsyncKosa8Client, sandbox_id: str) -> None:
        try:
            box = await client.get_sandbox(sandbox_id)
        except Kosa8Error as error:
            if _is_gone(error):
                raise WorkspaceUnavailableError(f'kosa8 sandbox {sandbox_id!r} no longer exists') from error
            raise
        state = box.get('state')
        if state in _GONE_STATES:
            raise WorkspaceUnavailableError(f'kosa8 sandbox {sandbox_id!r} is {state}')
        if state in _ASLEEP_STATES:
            try:
                await client.wake_sandbox(sandbox_id)
            except Kosa8Error as error:
                if _is_gone(error):
                    raise WorkspaceUnavailableError(f'kosa8 sandbox {sandbox_id!r} no longer exists') from error
                raise

    async def _exec(self, request: ExecRequest) -> ExecOutput:
        assert self._ref is not None
        sandbox_id = self._ref.id
        try:
            return await self.client().exec(sandbox_id, request)
        except Kosa8TimeoutError:
            raise
        except Kosa8Error as error:
            await self._raise_if_gone(sandbox_id, error)
            raise

    async def _raise_if_gone(self, sandbox_id: str, error: Kosa8Error) -> None:
        if _is_gone(error):
            raise WorkspaceUnavailableError(f'kosa8 sandbox {sandbox_id!r} no longer exists') from error
        # A host error while the sandbox is being deleted can come before the API reports it gone,
        # so after a 5xx look again for a few seconds.
        checks = _GONE_CHECKS if error.status is not None and error.status >= 500 else 1
        for attempt in range(checks):
            if attempt:
                await anyio.sleep(_GONE_CHECK_INTERVAL_S)
            try:
                box = await self.client().get_sandbox(sandbox_id)
            except Kosa8Error as check:
                if _is_gone(check):
                    raise WorkspaceUnavailableError(f'kosa8 sandbox {sandbox_id!r} no longer exists') from error
                return
            if box.get('state') in _GONE_STATES:
                raise WorkspaceUnavailableError(f'kosa8 sandbox {sandbox_id!r} is {box.get("state")}') from error

    # Commands ----------------------------------------------------------------

    async def run(
        self,
        command: WorkspaceCommand,
        *,
        shell: bool = False,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> CommandResult:
        if shell:
            if not isinstance(command, str):
                raise TypeError('shell=True takes a command string')
            script = command
        else:
            if isinstance(command, (str, bytes)) or not isinstance(command, Sequence):
                raise TypeError('a command string needs shell=True; pass an argv sequence otherwise')
            if not command:
                raise ValueError('argv must not be empty')
            if not all(isinstance(arg, str) for arg in command):
                raise TypeError('argv items must be strings')
            script = shlex.join(command)
        if timeout is not None and (
            isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0
        ):
            raise ValueError('timeout must be a positive finite number or None')
        user_env = {**self._env, **check_env(env)}

        await self._connect()
        assert self._working_dir is not None
        run_dir = f'{self._run_root}/{uuid.uuid4().hex}'
        k8: dict[str, str] = {
            'K8_RUN': run_dir,
            'K8_CWD': self._working_dir,
            'K8_INLINE': str(_INLINE_BYTES),
            'K8_CMD': script,
        }
        secs: int | None = None
        if timeout is not None:
            secs = max(1, math.ceil(timeout))
            k8['K8_TIMEOUT'] = str(secs)
        request = ExecRequest(argv=['/bin/sh', '-c', _LAUNCH, 'k8', _RUN, run_dir], env={**user_env, **k8})
        staged: str | None = None
        try:
            if len(json.dumps({'cmd': request.argv, 'env': [f'{k}={v}' for k, v in request.env.items()]})) > _BODY_LIMIT:
                staged = await self._stage(f'{run_dir}.sh', script.encode('utf-8'))
                del request.env['K8_CMD']
                request.env['K8_CMDFILE'] = staged
            out = await self._run_detached(request, run_dir, secs)
        except Kosa8TimeoutError as error:
            await self._kill(run_dir)
            raise WorkspaceTimeoutError(f'the command did not finish in time on kosa8: {error}') from error
        except BaseException:
            await self._kill(run_dir)
            raise
        return await self._result(out, run_dir, timeout)

    async def _run_detached(self, request: ExecRequest, run_dir: str, secs: int | None) -> ExecOutput:
        started = await self._exec(request)
        if started.exit_code != 0 or started.stdout.strip() != 'K8STARTED':
            detail = (started.stderr or started.stdout).strip()[:300]
            raise WorkspaceError(f'kosa8 could not start the command (exit {started.exit_code}): {detail}')
        deadline = None if secs is None else anyio.current_time() + secs + _SERVER_GRACE_S
        wait = ExecRequest(argv=['/bin/sh', '-c', _WAIT, 'k8', run_dir, str(_WAIT_S * 10)])
        while True:
            out = await self._exec(wait)
            if out.exit_code == 0 and out.stdout.strip() == 'K8WAIT':
                if deadline is not None and anyio.current_time() > deadline:
                    raise Kosa8TimeoutError('the command outlived its timeout in the sandbox')
                continue
            return ExecOutput(exit_code=out.exit_code, stdout=out.stdout, stderr=out.stderr)

    async def _result(self, out: ExecOutput, run_dir: str, timeout: float | None) -> CommandResult:
        if out.timed_out:
            raise WorkspaceTimeoutError('the kosa8 host stopped the command at its time limit', stdout='', stderr='')
        head, _, rest = out.stdout.partition('\n')
        fields = head.split()
        if fields[:1] == ['K8FAIL']:
            raise WorkspaceError(f'kosa8 could not start the command: {head[7:]}')
        if len(fields) != 5 or fields[0] != 'K8RUN' or out.exit_code != 0:
            detail = (out.stderr or out.stdout).strip()[:300]
            raise WorkspaceError(f'kosa8 returned an unexpected answer for the command (exit {out.exit_code}): {detail}')
        exit_code, timed_out, out_size, err_size = (int(value) for value in fields[1:])
        if out_size + err_size <= _INLINE_BYTES:
            encoded_out, sep, encoded_err = rest.partition('K8SEP')
            if not sep:
                raise WorkspaceError('kosa8 returned incomplete command output')
            stdout, stderr = _decode(encoded_out), _decode(encoded_err)
        else:
            try:
                stdout = await self._read_back(f'{run_dir}/o', min(out_size, _MAX_CAPTURE_BYTES))
                stderr = await self._read_back(f'{run_dir}/e', min(err_size, _MAX_CAPTURE_BYTES))
            finally:
                await self._exec(ExecRequest(argv=['rm', '-rf', run_dir]))
        if (len(stdout), len(stderr)) != (min(out_size, _MAX_CAPTURE_BYTES), min(err_size, _MAX_CAPTURE_BYTES)):
            raise WorkspaceError('kosa8 returned incomplete command output')
        text_out, text_err = stdout.decode('utf-8', 'replace'), stderr.decode('utf-8', 'replace')
        if timed_out:
            raise WorkspaceTimeoutError(f'the command did not finish within {timeout} s', stdout=text_out, stderr=text_err)
        if out_size > _MAX_CAPTURE_BYTES or err_size > _MAX_CAPTURE_BYTES:
            raise WorkspaceOutputLimitError(
                f'the command wrote more than {_MAX_CAPTURE_BYTES} bytes to one stream',
                limit=_MAX_CAPTURE_BYTES,
                stdout=text_out,
                stderr=text_err,
            )
        return CommandResult(exit_code=exit_code, stdout=text_out, stderr=text_err)

    async def _read_back(self, path: str, size: int) -> bytes:
        data = bytearray()
        index = 0
        while len(data) < size:
            out = await self._exec(ExecRequest(argv=['/bin/sh', '-c', _CHUNK, 'k8', path, str(_CHUNK_BYTES), str(index)]))
            if out.exit_code != 0:
                raise WorkspaceError(f'could not read command output from kosa8: {out.stderr.strip()[:300]}')
            chunk = _decode(out.stdout)
            if not chunk:
                break
            data.extend(chunk)
            index += 1
        return bytes(data[:size])

    async def _kill(self, run_dir: str) -> None:
        """Best effort, and shielded: the caller's cancellation or error still propagates."""
        if self._ref is None:
            return
        with anyio.move_on_after(_KILL_TIMEOUT_S, shield=True):
            try:
                await self.client().exec(self._ref.id, ExecRequest(argv=['/bin/sh', '-c', _KILL, 'k8', run_dir]))
            except Exception:
                pass

    async def _stage(self, path: str, data: bytes) -> str:
        """Put a command too large for one request body into a file in the sandbox; return its path."""
        assert self._ref is not None
        client = self.client()
        if self._files_api is not False and client.files_api is not False:
            files_path = posixpath.join(client.files_root, '.kosa8-run', posixpath.basename(path))
            try:
                await client.make_dir(self._ref.id, posixpath.dirname(files_path))
                await client.put_file(self._ref.id, files_path, data)
                self._files_api = True
                return files_path
            except Kosa8Error as error:
                if error.status == 404:
                    await self._raise_if_gone(self._ref.id, Kosa8Error('sandbox check', status=None))
                elif error.status not in (400, 501):
                    raise
                self._files_api = False
        for index, start in enumerate(range(0, max(len(data), 1), _STAGE_CHUNK_BYTES)):
            part = data[start : start + _STAGE_CHUNK_BYTES]
            out = await self._exec(
                ExecRequest(
                    argv=['/bin/sh', '-c', _STAGE, 'k8', path, 'w' if index == 0 else 'a'],
                    env={'K8_DATA': base64.b64encode(part).decode('ascii')},
                )
            )
            if out.exit_code != 0:
                raise WorkspaceError(f'could not stage a large command in kosa8 sandbox: {out.stderr.strip()[:300]}')
        return path


async def destroy_sandbox(client: AsyncKosa8Client, sandbox_id: str) -> None:
    """Delete a sandbox; one that no longer exists is already gone."""
    await client.delete_sandbox(check_sandbox_id(sandbox_id), missing_ok=True)
