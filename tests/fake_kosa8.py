"""An async stand-in for the kosa8 Cloud API, for tests.

It answers the endpoints the backend uses through an `httpx.MockTransport`. Exec requests
really run, as local subprocesses in a session of their own, so the backend's shell wrapper is
exercised end to end; sandbox paths are host paths. It enforces the API limits that matter here:
a 64 KiB JSON body, 8192 characters per argument, 128 environment entries, and a buffered
`timeout_s` of at most 90 s. Like a host whose client went away, it does not stop a command when
its exec request is cancelled; deleting a sandbox sends SIGTERM to its running execs and answers
them 404, as the sandbox is gone.

Nothing here talks to the network.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
from dataclasses import dataclass, field
from typing import Any

import httpx

BODY_LIMIT = 64 * 1024


@dataclass
class FakeKosa8:
    api_key: str = 'k8c_live_test'
    files_api: bool = True
    files_root: str = '/workspace'
    sandboxes: dict[str, dict[str, Any]] = field(default_factory=dict)
    requests: list[tuple[str, str]] = field(default_factory=list)
    #: Prepended to PATH for commands, e.g. a directory with a `setsid` for hosts (macOS) without one.
    path_prefix: str | None = None
    _procs: dict[str, set[asyncio.subprocess.Process]] = field(default_factory=dict)
    _n: int = 0

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(lambda request: self.handle(request))  # late-bound, so tests can wrap `handle`

    def async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self.transport())

    async def reap(self) -> None:
        for procs in self._procs.values():
            for proc in list(procs):
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
                with contextlib.suppress(Exception):
                    await proc.wait()

    async def handle(self, request: httpx.Request) -> httpx.Response:
        if request.headers.get('authorization') != f'Bearer {self.api_key}':
            return _err(401, 'unauthorized', 'bad key')
        raw = await request.aread()
        method, path = request.method, request.url.path
        self.requests.append((method, path))
        parts = [p for p in path.split('/') if p]
        if parts[:2] != ['v1', 'sandboxes']:
            return _err(404, 'not_found', 'no such route')
        if len(parts) == 2 and method == 'POST':
            return self._create(json.loads(raw) if raw else {})
        if len(parts) < 3:
            return _err(404, 'not_found', 'no such route')
        box = self.sandboxes.get(parts[2])
        if box is None or box['state'] == 'deleted':
            return _err(404, 'not_found', 'no such sandbox')
        action = parts[3] if len(parts) > 3 else ''
        if action == 'files':
            return self._files(box, parts, method, request, raw)
        if len(raw) > BODY_LIMIT:
            return _err(413, 'too_large', f'body over {BODY_LIMIT} bytes')
        body = json.loads(raw) if raw else {}
        if not action:
            if method == 'GET':
                return _json(200, {'sandbox': box})
            if method == 'DELETE':
                return await self._delete(box)
        if action == 'sleep' and method == 'POST':
            box['state'] = 'sleeping'
            return _json(200, {'sandbox': box})
        if action == 'wake' and method == 'POST':
            box['state'] = 'running'
            return _json(200, {'sandbox': box})
        if action == 'exec' and method == 'POST':
            return await self._exec(box, body)
        return _err(404, 'not_found', 'no such route')

    def _create(self, body: dict[str, Any]) -> httpx.Response:
        self._n += 1
        sid = f'sbx_test{self._n}'
        box = {'id': sid, 'state': 'running', 'template': body.get('template', 'base'), 'ttl_s': body.get('ttl_s')}
        self.sandboxes[sid] = box
        return _json(201, {'sandbox': box})

    async def _delete(self, box: dict[str, Any]) -> httpx.Response:
        box['state'] = 'deleted'
        for proc in list(self._procs.get(box['id'], ())):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGTERM)
        return _json(200, {'ok': True, 'id': box['id']})

    def _files(self, box: dict[str, Any], parts: list[str], method: str, request: httpx.Request, raw: bytes) -> httpx.Response:
        if not self.files_api:
            return _err(501, 'not_available', 'Files is not available on this host yet')
        mkdir = parts[4:] == ['mkdir']
        p = (json.loads(raw) if raw else {}).get('path', '') if mkdir else request.url.params.get('path', '')
        root = self.files_root.rstrip('/')
        if not (p == root or p.startswith(root + '/')) or '/../' in p + '/':
            return _err(400, 'invalid_path', 'path must be a workspace path')
        try:
            if mkdir and method == 'POST':
                os.makedirs(p, exist_ok=True)
                return _json(200, {'file': {'path': p, 'dir': True}})
            if len(parts) == 4 and method == 'PUT':
                with open(p, 'wb') as f:
                    f.write(raw)
                return _json(200, {'file': {'path': p, 'size': len(raw)}})
            if len(parts) == 4 and method == 'GET':
                with open(p, 'rb') as f:
                    return httpx.Response(200, content=f.read())
        except FileNotFoundError:
            return _err(404, 'not_found', f'no such file {p}')
        except OSError as error:
            return _err(400, 'bad_request', str(error))
        return _err(404, 'not_found', 'no such route')

    async def _exec(self, box: dict[str, Any], body: dict[str, Any]) -> httpx.Response:
        cmd = body.get('cmd')
        if not isinstance(cmd, list) or not cmd or not all(isinstance(a, str) and len(a) <= 8192 for a in cmd):
            return _err(400, 'invalid_cmd', 'cmd must be a non-empty list of strings of at most 8192 characters')
        envl = body.get('env', [])
        if len(envl) > 128:
            return _err(400, 'invalid_env', 'too many env entries')
        if body.get('stream'):
            return _err(501, 'not_available', 'Streamed exec is not available on this host yet')
        timeout_s = body.get('timeout_s')
        if timeout_s is not None and (not isinstance(timeout_s, int) or not 1 <= timeout_s <= 90):
            return _err(400, 'invalid_timeout', 'timeout_s is 1 to 90 on a buffered exec')
        box['state'] = 'running'
        path = os.environ.get('PATH', '/usr/bin:/bin')
        env = {'PATH': f'{self.path_prefix}:{path}' if self.path_prefix else path, 'HOME': os.environ.get('HOME', '/')}
        for entry in envl:
            k, _, v = entry.partition('=')
            env[k] = v
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        procs = self._procs.setdefault(box['id'], set())
        procs.add(proc)
        task = asyncio.ensure_future(proc.communicate())
        try:
            done, _ = await asyncio.wait({task}, timeout=timeout_s or 90)
        except asyncio.CancelledError:
            # The client went away; the command keeps running, as on a host that does not stop it.
            task.add_done_callback(lambda _t: procs.discard(proc))
            raise
        if not done:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            out, err = await task
            procs.discard(proc)
            return _json(504, {'error': 'exec_timeout', 'message': 'the command ran longer than its limit and was stopped'})
        procs.discard(proc)
        out, err = task.result()
        if box['state'] == 'deleted':
            return _err(404, 'not_found', 'no such sandbox')
        code = proc.returncode if proc.returncode is not None and proc.returncode >= 0 else 128 - (proc.returncode or 0)
        return _json(200, {'exit': code, 'stdout': out.decode('utf-8', 'replace'), 'stderr': err.decode('utf-8', 'replace')})


def _json(status: int, body: Any) -> httpx.Response:
    return httpx.Response(status, json=body)


def _err(status: int, code: str, message: str) -> httpx.Response:
    return httpx.Response(status, json={'error': code, 'message': message})
