# Changelog

## 0.1.0

- First version: `Kosa8Sandbox` capability and `Kosa8WorkspaceBackend` (`SupportsCommands`; file operations derived by Pydantic AI's `Workspace`), passing `WorkspaceBackendSuite` against a fake kosa8 API and live against kosa8 Cloud (38 passed, 2 skipped).
- Commands run detached over buffered exec and are waited on in 50 s polls, so they can outlive one buffered request and share the sandbox's processes and `/tmp` with later requests (kosa8's streamed exec can run in a namespace of its own).
- A host error while the sandbox is being deleted is rechecked for a few seconds and reported as `WorkspaceUnavailableError`.
