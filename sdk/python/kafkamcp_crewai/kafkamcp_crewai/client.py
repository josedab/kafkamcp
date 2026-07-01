"""Synchronous MCP client adapters for CrewAI."""

from contextlib import AbstractContextManager
from typing import Any, Optional

from anyio.from_thread import BlockingPortal, start_blocking_portal
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class StdioMCPClient(AbstractContextManager["StdioMCPClient"]):
    """Run an MCP stdio session on a worker event loop for synchronous crews."""

    def __init__(
        self,
        command: str = "kafkamcp",
        args: Optional[list[str]] = None,
        env: Optional[dict[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> None:
        self._server = StdioServerParameters(
            command=command,
            args=args or [],
            env=env,
            cwd=cwd,
        )
        self._portal_context: Any = None
        self._portal: Optional[BlockingPortal] = None
        self._stdio_context: Any = None
        self._session_context: Any = None
        self._session: Optional[ClientSession] = None

    def __enter__(self) -> "StdioMCPClient":
        if self._portal is not None:
            raise RuntimeError("StdioMCPClient is already open")

        self._portal_context = start_blocking_portal()
        self._portal = self._portal_context.__enter__()
        try:
            stdio_context = self._portal.wrap_async_context_manager(
                stdio_client(self._server)
            )
            read_stream, write_stream = stdio_context.__enter__()
            self._stdio_context = stdio_context

            session_context = self._portal.wrap_async_context_manager(
                ClientSession(read_stream, write_stream)
            )
            self._session = session_context.__enter__()
            self._session_context = session_context
            self._portal.call(self._session.initialize)
        except BaseException:
            self.close()
            raise
        return self

    def call_tool(self, name: str, args: dict[str, Any]) -> Any:
        """Invoke a tool synchronously on the managed MCP session."""
        if self._portal is None or self._session is None:
            raise RuntimeError("StdioMCPClient must be used as a context manager")
        return self._portal.call(self._session.call_tool, name, args)

    def close(self) -> None:
        """Close the MCP session and its worker event loop."""
        if self._portal is None:
            return

        portal_context = self._portal_context
        try:
            try:
                if self._session_context is not None:
                    self._session_context.__exit__(None, None, None)
            finally:
                if self._stdio_context is not None:
                    self._stdio_context.__exit__(None, None, None)
        finally:
            self._session = None
            self._session_context = None
            self._stdio_context = None
            self._portal = None
            self._portal_context = None
            portal_context.__exit__(None, None, None)

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
