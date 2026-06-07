"""Editor-extension distribution: install the bundled AMX VSIX.

The AMX editor extension ships *inside* the amx-cli wheel (vendored
VSIX under ``amx/assets/vsix/``) so end users need neither Node nor a
Marketplace account, and the extension version always matches the
server it talks to.

Public surface lives in :mod:`amx.vscode_ext.installer`; the ``/vscode``
REPL command and the Studio ``/api/vscode`` router are thin layers over
it.
"""

from amx.vscode_ext.installer import (
    EXTENSION_ID,
    EditorInfo,
    InstallerError,
    bundled_vsix_path,
    bundled_vsix_version,
    discover_editors,
    extension_status,
    install,
    uninstall,
)

__all__ = [
    "EXTENSION_ID",
    "EditorInfo",
    "InstallerError",
    "bundled_vsix_path",
    "bundled_vsix_version",
    "discover_editors",
    "extension_status",
    "install",
    "uninstall",
]
