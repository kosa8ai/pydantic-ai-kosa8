"""kosa8 Cloud sandboxes as Pydantic AI workspaces.

`Kosa8Sandbox` is the capability to add to an agent; `Kosa8WorkspaceBackend` is the
`WorkspaceBackend` it supplies, public for applications that create or attach to a sandbox
themselves and pass it to a run as `workspace=`.
"""

from ._backend import Kosa8WorkspaceBackend
from ._capability import Kosa8Sandbox
from ._kosa8 import (
    AsyncKosa8Client,
    Kosa8Error,
    Kosa8ExecTimeoutError,
    Kosa8NotSupportedError,
    Kosa8OrgFrozenError,
    Kosa8PaymentRequiredError,
    Kosa8RateLimitError,
    Kosa8TimeoutError,
)

__version__ = '0.1.0'

__all__ = [
    'AsyncKosa8Client',
    'Kosa8Error',
    'Kosa8ExecTimeoutError',
    'Kosa8NotSupportedError',
    'Kosa8OrgFrozenError',
    'Kosa8PaymentRequiredError',
    'Kosa8RateLimitError',
    'Kosa8Sandbox',
    'Kosa8TimeoutError',
    'Kosa8WorkspaceBackend',
]
